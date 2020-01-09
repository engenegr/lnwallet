package com.lightning.walletapp

import scala.concurrent.duration._
import com.lightning.walletapp.ln._
import com.lightning.walletapp.ln.wire._
import com.lightning.walletapp.lnutils._
import com.lightning.walletapp.ln.Channel._
import com.lightning.walletapp.ln.Scripts._
import com.lightning.walletapp.ln.LNParams._
import com.lightning.walletapp.ln.PaymentInfo._
import com.lightning.walletapp.lnutils.ImplicitConversions._
import com.lightning.walletapp.ln.wire.LightningMessageCodecs._

import rx.lang.scala.{Observable => Obs}
import scodec.bits.{BitVector, ByteVector}
import fr.acinq.bitcoin.Crypto.{Point, PublicKey}
import com.lightning.walletapp.ln.Tools.{runAnd, none}
import com.lightning.walletapp.helper.{RichCursor, ThrottledWork}
import com.lightning.walletapp.lnutils.JsonHttpUtils.{ioQueue, pickInc, retry}
import org.bitcoinj.core.{Block, Coin, FilteredBlock, Peer, Sha256Hash, Transaction => JTransaction}
import fr.acinq.bitcoin.{SatoshiLong, Transaction => STransaction}

import org.bitcoinj.core.TransactionConfidence.ConfidenceType.DEAD
import com.lightning.walletapp.ln.crypto.Sphinx.PublicKeyVec
import com.lightning.walletapp.ln.PaymentInfo.FullOrEmptyRD
import com.lightning.walletapp.lnutils.olympus.TxUploadAct
import com.lightning.walletapp.Utils.app
import org.bitcoinj.wallet.Wallet
import scala.concurrent.Future
import scodec.DecodeResult


object ChannelManager extends ChannelListener {
  val operationalListeners = Set(ChannelManager, RatesSaver, bag)
  val CMDLocalShutdown = CMDShutdown(scriptPubKey = None)
  var currentBlocksLeft = Option.empty[Int]

  var all: Vector[Channel] = ChannelWrap doGet db collect {
    case data: HasNormalCommits => createChannel(operationalListeners, data)
    case data: HostedCommits => createHostedChannel(operationalListeners, data)
    case data => throw new RuntimeException(s"Can't create channel with $data")
  }

  val chainEventsListener = new TxTracker with BlocksListener {
    override def onChainDownloadStarted(peer: Peer, left: Int) = onBlock(left)
    def onBlocksDownloaded(peer: Peer, b: Block, fb: FilteredBlock, left: Int) = onBlock(left)
    def onCoinsReceived(wallet: Wallet, tx: JTransaction, a: Coin, b: Coin) = onChainTx(CMDSpent apply tx)
    def onCoinsSent(wallet: Wallet, tx: JTransaction, a: Coin, b: Coin) = onChainTx(CMDSpent apply tx)

    def onBlock(blocksLeft: Int) = {
      val firstCall = currentBlocksLeft.isEmpty
      currentBlocksLeft = Some(blocksLeft)

      // Let channels know immediately, important for hosted ones
      // then also repeat on each next incoming last block to save resouces
      if (firstCall || blocksLeft < 1) all.foreach(_ process CMDChainTipKnown)
      // Don't call this on each new block but only on (re)connection
      if (firstCall) PaymentInfoWrap.resolvePending
    }

    def onChainTx(cmdSpent: CMDSpent) = {
      for (chan <- all) chan process cmdSpent
      bag.extractPreimage(cmdSpent.tx)
    }
  }

  ConnectionManager.listeners += new ConnectionListener {
    override def onOperational(nodeId: PublicKey, isCompat: Boolean) = fromNode(nodeId).foreach(_ process CMDSocketOnline)
    override def onHostedMessage(ann: NodeAnnouncement, msg: HostedChannelMessage) = findById(fromNode(ann.nodeId), ann.hostedChanId).foreach(_ process msg)

    override def onMessage(nodeId: PublicKey, msg: LightningMessage) = msg match {
      case channelUpdate: ChannelUpdate => fromNode(nodeId).foreach(_ process channelUpdate)
      case nodeError: Error if nodeError.channelId == fr.acinq.bitcoin.Protocol.Zeroes => fromNode(nodeId).foreach(_ process nodeError)
      case channelMessage: ChannelMessage => findById(fromNode(nodeId), channelMessage.channelId).foreach(_ process channelMessage)
      case _ =>
    }

    override def onDisconnect(nodeId: PublicKey) = {
      fromNode(nodeId).foreach(_ process CMDSocketOffline)
      Obs.just(null).delay(5.seconds).foreach(_ => initConnect)
    }
  }

  def perKwSixSat: Long = RatesSaver.rates.feeSix.value / 4
  def perKwThreeSat: Long = RatesSaver.rates.feeThree.value / 4
  def onChainThreshold = Scripts.weight2fee(perKwSixSat, weight = 750)
  def currentHeight: Int = app.kit.wallet.getLastBlockSeenHeight + currentBlocksLeft.getOrElse(0)
  def blockDaysLeft: Int = currentBlocksLeft.map(_ / blocksPerDay).getOrElse(Int.MaxValue)
  def currentBlockDay: Int = currentHeight / blocksPerDay

  def getTx(txid: ByteVector) = {
    val wrapped = Sha256Hash wrap txid.toArray
    Option(app.kit.wallet getTransaction wrapped)
  }

  def getStatus(txid: ByteVector) = getTx(txid) map { tx =>
    val isTxDead = tx.getConfidence.getConfidenceType == DEAD
    tx.getConfidence.getDepthInBlocks -> isTxDead
  } getOrElse 0 -> false

  // Parent state and next tier cltv delay
  // actual negative delay will be represented as 0L
  def cltv(parent: STransaction, child: STransaction) = {
    val parentDepth \ parentIsDead = getStatus(parent.txid)
    val cltvDelay = math.max(cltvBlocks(child) - currentHeight, 0L)
    parentDepth -> parentIsDead -> cltvDelay
  }

  // Parent state and cltv + next tier csv delay
  // actual negative delay will be represented as 0L
  def csv(parent: STransaction, child: STransaction) = {
    val parentDepth \ parentIsDead = getStatus(parent.txid)
    val cltvDelay = math.max(cltvBlocks(parent) - currentHeight, 0L)
    val csvDelay = math.max(csvTimeout(child) - parentDepth, 0L)
    parentDepth -> parentIsDead -> (cltvDelay + csvDelay)
  }

  def cltvReserveTooSmall(add: UpdateAddHtlc) = currentHeight + PaymentRequest.cltvExpiryTag.expiryDelta > add.expiry + 3 // Minus 3 to account for blocks arriving while payment is in flight
  def cltvExpiryCloseToExpiration(add: UpdateAddHtlc) = add.expiry - currentHeight < PaymentRequest.leeway // Once preimage is revealed but not settled in normal channel we'll break it if this is true
  def csvShowDelayed(t1: TransactionWithInputInfo, t2: TransactionWithInputInfo, commitTx: STransaction) = ShowDelayed(csv(t1.tx, t2.tx), t2.tx, commitTx, fee = t1 -- t2, t2.tx.allOutputsAmount)

  // INCOMING MPP RESOLUTION

  var listeners = Set.empty[ChannelManagerListener]
  protected[this] val events = new ChannelManagerListener {
    override def incomingReceived(amountMsat: Long, info: PaymentInfo) =
      for (lst <- listeners) lst.incomingReceived(amountMsat, info)
  }

  val incomingTimeoutWorker: ThrottledWork[ByteVector, Any] = new ThrottledWork[ByteVector, Any] {
    // We always resolve incoming payments in same channel thread to avoid concurrent messaging issues
    def process(hash: ByteVector, res: Any) = Future(incomingCrossSigned)(channelContext)
    def work(paymenthash: ByteVector) = ioQueue delay 60.seconds
    def error(canNotHappen: Throwable) = none
  }

  override def incomingCrossSigned = {
    val allIncomingPayments = all.flatMap(_.pendingIncoming).map(_.initResolution)
    val allOutgoingHashes = all.flatMap(_.pendingOutgoing).groupBy(_.paymentHash).mapValues(_.map(_.channelId).toSet)
    val badRightAway = allIncomingPayments collect { case badAddResolution: BadAddResolution => badAddResolution }
    val maybeGood = allIncomingPayments collect { case finalPayloadSpec: FinalPayloadSpec => finalPayloadSpec }

    val results = maybeGood.groupBy(_.add.paymentHash).map(_.swap).mapValues(bag.getPaymentInfo) map {
      case payments \ None => for (pay <- payments) yield failFinalPayloadSpec(pay.add.incorrectDetails, pay)
      case payments \ _ if payments.map(_.payload.totalAmountMsat).toSet.size > 1 => for (pay <- payments) yield failFinalPayloadSpec(pay.add.incorrectDetails, pay)
      case payments \ Some(info) if payments.exists(_.payload.totalAmountMsat < info.pr.msatOrMin.toLong) => for (pay <- payments) yield failFinalPayloadSpec(pay.add.incorrectDetails, pay)
      case payments \ _ if payments.exists(pay => allOutgoingHashes.get(pay.add.paymentHash) contains pay.add.channelId) => for (pay <- payments) yield failFinalPayloadSpec(PermanentChannelFailure, pay)
      case payments \ Some(info) if !payments.flatMap(_.payload.paymentSecretOpt).forall(info.pr.paymentSecretOpt.contains) => for (pay <- payments) yield failFinalPayloadSpec(pay.add.incorrectDetails, pay)

      case payments \ Some(info) if payments.map(_.add.amountMsat).sum >= info.pr.msatOrMin.toLong =>
        // We have collected enough incoming HTLCs to cover our requested amount, must fulfill them all
        events.incomingReceived(amountMsat = payments.map(_.add.amountMsat).sum, info)
        for (pay <- payments) yield CMDFulfillHtlc(info.paymentPreimage, pay.add)

      // This is an unexpected extra-payment or something like OFFLINE channel with fulfilled payment becoming OPEN, silently fullfil these
      case payments \ Some(info) if info.incoming == 1 && info.status == SUCCESS => for (pay <- payments) yield CMDFulfillHtlc(info.paymentPreimage, pay.add)
      case payments \ _ if incomingTimeoutWorker.hasFinishedOrNeverStarted => for (pay <- payments) yield failFinalPayloadSpec(PaymentTimeout, pay)
      case _ => Vector.empty
    }

    // Use `doProcess` to resolve all payments within this single call inside of channel executor
    // this whole method MUST itself be called within a channel executor to avoid concurrency issues
    for (cmd <- results.flatten) findById(all, cmd.add.channelId).foreach(_ doProcess cmd)
    for (cmd <- badRightAway) findById(all, cmd.add.channelId).foreach(_ doProcess cmd)
    for (chan <- all) chan doProcess CMDProceed
  }

  // CHANNEL LISTENER IMPLEMENTATION

  override def onProcessSuccess = {
    case (_: NormalChannel, close: ClosingData, _: CMDSpent | CMDChainTipKnown) =>
      val tier12Publishable = for (closingState <- close.tier12States if closingState.isPublishable) yield closingState.txn
      for (tx <- close.mutualClose ++ close.localCommit.map(_.commitTx) ++ tier12Publishable) try app.kit blockSend tx catch none

    case (chan: NormalChannel, wait: WaitFundingDoneData, CMDChainTipKnown) if wait.our.isEmpty =>
      // Once all pending blocks have been received, check if funding transaction which has been published earlier is confirmed by now
      getStatus(wait.fundingTx.txid) match { case depth \ _ if depth >= minDepth => chan process CMDConfirmed(wait.fundingTx) case _ => }

    // Failsafe check when channel has refunding data with remote point present but no spend seen, re-scheduling timeout timer on Adds
    case (chan: NormalChannel, ref: RefundingData, CMDChainTipKnown) if ref.remoteLatestPoint.isDefined => check1stLevelSpent(chan, ref)
    case (_, _, add: UpdateAddHtlc) => incomingTimeoutWorker.replaceWork(add.paymentHash)
  }

  override def onBecome = {
    case (_: NormalChannel, wait: WaitFundingDoneData, _, _) => app.kit blockSend wait.fundingTx
    case (_, _, WAIT_FOR_ACCEPT | SLEEPING, OPEN | CLOSING | SUSPENDED) => incomingCrossSigned
  }

  def delayedPublishes = {
    val statuses = all.map(_.data).collect { case cd: ClosingData => cd.bestClosing.getState }
    // Select all ShowDelayed which can't be published yet because cltv/csv delays are not cleared on them
    statuses.flatten.collect { case sd: ShowDelayed if !sd.isPublishable && sd.delay > Long.MinValue => sd }
  }

  // AIR

  def airCanSendInto(targetChan: Channel) = for {
    canSend <- all.filter(isOperational).diff(targetChan :: Nil).map(_.localUsableMsat)
    // While rebalancing, payments from other channels will lose some off-chain fee
    canSendFeeIncluded = canSend - maxAcceptableFee(canSend, hops = 3)
    // Estimation should be smaller than original but not negative
    if canSendFeeIncluded < canSend && canSendFeeIncluded > 0L
  } yield canSendFeeIncluded

  def estimateAIRCanSend = {
    // It's possible that balance from all channels will be less than most funded chan capacity
    val airCanSend = mostFundedChanOpt.map(chan => chan.estCanSendMsat + airCanSendInto(chan).sum) getOrElse 0L
    val usefulCaps = all.filter(isOperational).map(chan => chan.localUsableMsat.zeroIfNegative + chan.remoteUsableMsat.zeroIfNegative)
    // We are ultimately bound by the useful capacity (sendable + receivable - currently inflight payments) of the largest channel
    usefulCaps.reduceOption(_ max _) getOrElse 0L min airCanSend
  }

  // CHANNEL

  def hasHostedChanWith(nodeId: PublicKey) = fromNode(nodeId).exists(_.isHosted)
  def hasNormalChanWith(nodeId: PublicKey) = fromNode(nodeId).exists(chan => isOpeningOrOperational(chan) && !chan.isHosted)
  // We need to connect the rest of channels including special cases like REFUNDING normal channel and SUSPENDED hosted channel
  def initConnect = for (chan <- all if chan.state != CLOSING) ConnectionManager.connectTo(chan.data.announce, notify = false)
  def findById(from: Vector[Channel], chanId: ByteVector) = from.find(_.getCommits.map(_.channelId) contains chanId)
  def fromNode(nodeId: PublicKey) = for (chan <- all if chan.data.announce.nodeId == nodeId) yield chan

  def attachToAllChans(lst: ChannelListener) = for (chan <- all) chan.listeners += lst
  def detachFromAllChans(lst: ChannelListener) = for (chan <- all) chan.listeners -= lst

  def check2ndLevelSpent(chan: NormalChannel, cd: ClosingData) =
    retry(olympusWrap getChildTxs cd.commitTxs.map(_.txid), pickInc, 7 to 8).foreach(txs => {
      // In case of breach after publishing a revoked remote commit our peer may further publish Timeout and Success HTLC outputs
      // our job here is to watch for every output of every revoked commit tx and re-spend it before their CSV delay runs out
      for (tx <- txs) chan process CMDSpent(tx)
      for (tx <- txs) bag.extractPreimage(tx)
    }, none)

  def check1stLevelSpent(chan: NormalChannel, some: HasNormalCommits) =
    retry(olympusWrap getChildTxs Seq(some.commitments.commitInput.outPoint.txid), pickInc, 7 to 8).foreach(txs => {
      // Failsafe check in case if peer has already closed this channel unilaterally while us being offline at that time
      for (tx <- txs) chan process CMDSpent(tx)
    }, none)

  // CREATING CHANNELS

  val backupSaveWorker = new ThrottledWork[String, Any] {
    def work(cmd: String) = ioQueue delay 4.seconds
    def error(canNotHappen: Throwable) = none

    def process(cmd: String, res: Any): Unit = if (LocalBackup.isExternalStorageWritable) try {
      val backupFileUnsafe = LocalBackup.getBackupFileUnsafe(LocalBackup getBackupDirectory chainHash)
      LocalBackup.encryptAndWrite(backupFileUnsafe, all, app.kit.wallet, keys.cloudSecret)
    } catch none
  }

  def createHostedChannel(initListeners: Set[ChannelListener], bootstrap: ChannelData) = new HostedChannel {
    // A trusted but privacy-preserving and auditable channel, does not require any on-chain management

    def STORE[T <: ChannelData](hostedCommitments: T) = {
      backupSaveWorker replaceWork "HOSTED-INIT-SAVE-BACKUP"
      ChannelWrap put hostedCommitments
      hostedCommitments
    }

    listeners = initListeners
    doProcess(bootstrap)
  }

  def createChannel(initListeners: Set[ChannelListener], bootstrap: ChannelData) = new NormalChannel {
    // A trustless channel which has on-chain funding, requires history and on-chain state management

    def STORE[T <: ChannelData](normalCommitments: T) = {
      backupSaveWorker replaceWork "NORMAL-INIT-SAVE-BACKUP"
      ChannelWrap put normalCommitments
      normalCommitments
    }

    def REV(cs: NormalCommits, rev: RevokeAndAck) = for {
      tx <- cs.remoteCommit.txOpt // We use old commitments to save a punishment for remote commit before it gets dropped
      myBalance = cs.remoteCommit.spec.toRemoteMsat // Local commit has been cleared by now, remote commit still has relevant balance
      revocationInfo = Helpers.Closing.makeRevocationInfo(cs, tx, rev.perCommitmentSecret, perKwThreeSat * 3) // Scorched earth fee policy
      serialized = LightningMessageCodecs.serialize(revocationInfoCodec encode revocationInfo)
    } db.change(RevokedInfoTable.newSql, tx.txid, cs.channelId, myBalance, serialized)

    def GETREV(cs: NormalCommits, tx: fr.acinq.bitcoin.Transaction) = {
      val databaseCursor = db.select(RevokedInfoTable.selectTxIdSql, tx.txid)
      val rc = RichCursor(databaseCursor).headTry(_ string RevokedInfoTable.info)

      for {
        serialized <- rc.toOption
        bitVec = BitVector.fromValidHex(serialized)
        DecodeResult(ri, _) <- revocationInfoCodec.decode(bitVec).toOption
        riWithRefreshedFeerate = ri.copy(feeRate = ri.feeRate max perKwThreeSat)
        perCommitmentSecret <- Helpers.Closing.extractCommitmentSecret(cs, commitTx = tx)
        ri1 = Helpers.Closing.reMakeRevocationInfo(riWithRefreshedFeerate, cs, tx, perCommitmentSecret)
      } yield Helpers.Closing.claimRevokedRemoteCommitTxOutputs(ri1, tx)
    }

    def CLOSEANDWATCH(cd: ClosingData) = {
      val txs = cd.tier12States.collect { case status: DelayedPublishStatus => status.txn.bin }.toVector
      if (txs.nonEmpty) olympusWrap tellClouds TxUploadAct(txvec.encode(txs).require.toByteVector, Nil, "txs/schedule")
      // Collect all the commit txs publicKeyScripts and watch them since remote peer may reveal preimages on-chain
      app.kit.wallet.addWatchedScripts(app.kit closingPubKeyScripts cd)
      check2ndLevelSpent(this, cd)
      BECOME(STORE(cd), CLOSING)
    }

    def ASKREFUNDPEER(some: HasNormalCommits, point: Point) = {
      val msg = ByteVector.fromValidHex("please publish your local commitment".s2hex)
      val ref = RefundingData(some.announce, Some(point), some.commitments)
      val error = Error(some.commitments.channelId, msg)

      // Send both invalid reestablish and an error
      app.kit.wallet.addWatchedScripts(app.kit fundingPubScript some)
      SEND(makeReestablish(some, nextLocalCommitmentNumber = 0L), error)
      BECOME(STORE(ref), REFUNDING)
    }

    listeners = initListeners
    doProcess(bootstrap)
  }

  // SENDING PAYMENTS

  def checkIfSendable(rd: RoutingData) = {
    val paymentInfoOpt = bag.getPaymentInfo(rd.pr.paymentHash)
    val toSelfSuccess = rd.isReflexive && paymentInfoOpt.exists(_.status == SUCCESS) // We have already sent this payment to ourselves so preimage is revealed and it's risky to send again
    val preimageRisk = !rd.isReflexive && paymentInfoOpt.exists(_.paymentPreimage != NOIMAGE) // We have an incoming-pending (not SUCCESS) / filfilled-outgoing payment with identical preimage

    if (toSelfSuccess || preimageRisk) Left(R.string.err_ln_fulfilled, NOT_SENDABLE) else mostFundedChanOpt.map(_.estCanSendMsat) match {
      case Some(max) if max < rd.firstMsat && rd.airLeft > 1 && estimateAIRCanSend >= rd.firstMsat => Left(R.string.dialog_sum_big, SENDABLE_AIR)
      case Some(max) if max < rd.firstMsat => Left(R.string.dialog_sum_big, NOT_SENDABLE)
      case None => Left(R.string.dialog_sum_big, NOT_SENDABLE)
      case _ => Right(rd)
    }
  }

  def fetchRoutes(rd: RoutingData) = {
    val from: PublicKeyVec = rd.fromHostedOnly match {
      case false => all.filter(chan => isOperational(chan) && chan.localUsableMsat >= rd.firstMsat).map(_.data.announce.nodeId).distinct
      case true => all.filter(chan => isOperational(chan) && chan.localUsableMsat >= rd.firstMsat && chan.isHosted).map(_.data.announce.nodeId).distinct
    }

    def withHints = for {
      tag <- Obs from rd.pr.routingInfo
      partialRoutes <- getRoutes(tag.route.head.nodeId)
    } yield Obs just partialRoutes.map(_ ++ tag.route)

    def getRoutes(target: PublicKey) =
      if (rd.isReflexive) from diff Vector(target) match {
        case restFrom if restFrom.isEmpty => Obs just Vector(Vector.empty)
        case restFrom => BadEntityWrap.findRoutes(restFrom, target, rd)
      } else from contains target match {
        case false => BadEntityWrap.findRoutes(from, target, rd)
        case true => Obs just Vector(Vector.empty)
      }

    val paymentRoutesObs =
      if (from.isEmpty) Obs error new LightningException("No sources")
      else if (rd.isReflexive) Obs.zip(withHints).map(_.flatten.toVector)
      else Obs.zip(getRoutes(rd.pr.nodeId) +: withHints).map(_.flatten.toVector)

    for {
      rs <- paymentRoutesObs
      // Channel could have been operational when we were asking for a route but got closed later, so there always must be a default value for non-existing keys
      openMap = Tools.toDefMap[Channel, PublicKey, Int](all.filter(isOperational), _.data.announce.nodeId, chan => if (chan.state == OPEN) 0 else 1, default = 1)
      busyMap = Tools.toDefMap[Channel, PublicKey, Int](all.filter(isOperational), _.data.announce.nodeId, chan => chan.pendingOutgoing.size, default = 1)
      foundRoutes = rs.sortBy(totalRouteFee(_, 10000000L).abs).sortBy(openMap compose rd.nextNodeId).sortBy(busyMap compose rd.nextNodeId)
      // We may have out of band pre-filled routes in RD so append them to found ones
    } yield useFirstRoute(foundRoutes ++ rd.routes, rd)
  }

  def sendEither(foeRD: FullOrEmptyRD, noRoutes: RoutingData => Unit): Unit = foeRD match {
    // Find a channel which can send an amount and belongs to a correct peer, not necessairly online
    case Left(emptyRD) => noRoutes(emptyRD)

    case Right(rd) =>
      all.filter(isOperational) find { chan =>
        val matchesHostedOnlyPolicy = if (rd.fromHostedOnly) chan.isHosted else true // User may desire to only spend from hosted channels
        val correctTargetPeerNodeId = chan.data.announce.nodeId == rd.nextNodeId(rd.usedRoute) // Onion does not care about specific chan but peer nodeId must match
        matchesHostedOnlyPolicy && correctTargetPeerNodeId && chan.localUsableMsat >= rd.firstMsat // Balance may change while payment is being prepared
      } match {
        case None => sendEither(useFirstRoute(rd.routes, rd), noRoutes)
        case Some(targetGoodChannel) => targetGoodChannel process rd
      }
  }
}

trait ChannelManagerListener {
  def incomingReceived(amountMsat: Long, info: PaymentInfo): Unit = none
}
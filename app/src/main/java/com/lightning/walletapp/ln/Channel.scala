package com.lightning.walletapp.ln

import com.softwaremill.quicklens._
import com.lightning.walletapp.ln.wire._
import com.lightning.walletapp.ln.Channel._
import com.lightning.walletapp.ln.ChanErrorCodes._

import fr.acinq.eclair.UInt64
import scodec.bits.ByteVector
import scala.concurrent.Future
import java.util.concurrent.Executors
import fr.acinq.bitcoin.Protocol.Zeroes
import com.lightning.walletapp.ln.Tools.none
import com.lightning.walletapp.ChannelManager
import fr.acinq.bitcoin.ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS
import com.lightning.walletapp.ln.wire.LightningMessageCodecs.LNDirectionalMessage
import com.lightning.walletapp.ln.crypto.{Generators, ShaChain, ShaHashesWithIndex}
import com.lightning.walletapp.ln.Helpers.{Closing, Funding}
import fr.acinq.bitcoin.Crypto.{Point, Scalar}
import fr.acinq.bitcoin.{Satoshi, Transaction}
import scala.util.{Failure, Success, Try}


object Channel {
  val WAIT_FOR_INIT = "WAIT-FOR-INIT"
  val WAIT_FOR_ACCEPT = "WAIT-FOR-ACCEPT"
  val WAIT_FUNDING_SIGNED = "WAIT-FUNDING-SIGNED"

  // All states below are persisted
  val WAIT_FUNDING_DONE = "WAIT-FUNDING-DONE"
  val NEGOTIATIONS = "NEGOTIATIONS"
  val SLEEPING = "SLEEPING"
  val OPEN = "OPEN"

  // No tears, only dreams now
  val SUSPENDED = "SUSPENDED"
  val REFUNDING = "REFUNDING"
  val CLOSING = "CLOSING"

  // Single stacking thread for all channels, must be used when asking channels for pending payments to avoid race conditions
  implicit val channelContext: scala.concurrent.ExecutionContextExecutor = scala.concurrent.ExecutionContext fromExecutor Executors.newSingleThreadExecutor
  def isOperational(chan: Channel) = chan.data match { case NormalData(_, _, None, None, None) => true case hc: HostedCommits => hc.getError.isEmpty case _ => false }
  def isOpeningOrOperational(chan: Channel) = isOperational(chan) || isOpening(chan)
  def isOpening(chan: Channel) = chan.data.isInstanceOf[WaitFundingDoneData]

  def channelAndHop(chan: Channel) = for {
    // Only proceed if channel has (1) commits
    // and (2) remote update supplied by peer

    commits <- chan.getCommits
    chanUpdate <- commits.updateOpt
    nodeId = chan.data.announce.nodeId
    path = Vector(chanUpdate toHop nodeId)
  } yield chan -> path
}

abstract class Channel(val isHosted: Boolean) extends StateMachine[ChannelData] {
  def process(changes: Any*) = Future(changes foreach doProcess) onFailure { case fail => events onException this -> fail }
  def SEND(msg: LightningMessage*) = for (work <- ConnectionManager.workers get data.announce.nodeId) msg foreach work.handler.process

  def getCommits: Option[Commitments] = data match {
    case normal: HasNormalCommits => Some(normal.commitments)
    case hosted: HostedCommits => Some(hosted)
    case _ => None
  }

  def isUpdatable(upd: ChannelUpdate) =
    getCommits.flatMap(_.updateOpt) forall { oldUpdate =>
      val isDifferentShortId = oldUpdate.shortChannelId != upd.shortChannelId
      val isOldUpdateRefresh = !isDifferentShortId && oldUpdate.timestamp < upd.timestamp
      isDifferentShortId || isOldUpdateRefresh
    }

  def BECOME(data1: ChannelData, state1: String) = {
    // Transition must be defined before vars are updated
    val trans = Tuple4(this, data1, state, state1)
    super.become(data1, state1)
    events.onBecome(trans)
  }

  def STORESENDBECOME(data1: ChannelData, state1: String, lnMessage: LightningMessage*) = {
    // In certain situations we need this specific sequence, it's a helper method to make it oneliner
    // store goes first to ensure we retain an updated data before revealing it if anything goes wrong

    STORE(data1)
    SEND(lnMessage:_*)
    BECOME(data1, state1)
  }

  def STORE[T <: ChannelData](data: T): T // Persist channel data
  def pendingIncoming: Set[UpdateAddHtlc] // Cross-signed but not yet resolved by us
  def pendingOutgoing: Set[UpdateAddHtlc] // Signed + new payments offered by us
  def remoteUsableMsat: Long
  def localUsableMsat: Long
  def refundableMsat: Long

  var waitingUpdate: Boolean = true
  var permanentOffline: Boolean = true
  var listeners: Set[ChannelListener] = _
  val events: ChannelListener = new ChannelListener {
    override def onProcessSuccess = { case ps => for (lst <- listeners if lst.onProcessSuccess isDefinedAt ps) lst onProcessSuccess ps }
    override def onException = { case failure => for (lst <- listeners if lst.onException isDefinedAt failure) lst onException failure }
    override def onBecome = { case transition => for (lst <- listeners if lst.onBecome isDefinedAt transition) lst onBecome transition }
    override def fulfillReceived(fulfill: UpdateFulfillHtlc) = for (lst <- listeners) lst fulfillReceived fulfill
    override def incomingCrossSigned = for (lst <- listeners) lst.incomingCrossSigned
  }
}

abstract class NormalChannel extends Channel(isHosted = false) { me =>
  def fundTxId = data match { case hnc: HasNormalCommits => hnc.commitments.commitInput.outPoint.txid case _ => ByteVector.empty }
  def pendingOutgoing = data match { case hnc: HasNormalCommits => hnc.commitments.localSpec.outgoingAdds ++ hnc.commitments.remoteCommit.spec.incomingAdds ++ hnc.commitments.localChanges.adds case _ => Set.empty }
  def pendingIncoming = data match { case hnc: HasNormalCommits => hnc.commitments.reducedRemoteSpec.outgoingAdds case _ => Set.empty } // Will take into account local proposed Fail/Fulfill in CLOSED state
  def remoteUsableMsat = data match { case hnc: HasNormalCommits => hnc.commitments.reducedRemoteState.canReceiveMsat case _ => 0L }
  def localUsableMsat = data match { case hnc: HasNormalCommits => hnc.commitments.reducedRemoteState.canSendMsat case _ => 0L }
  def refundableMsat = data match { case hnc: HasNormalCommits => hnc.commitments.localCommit.spec.toLocalMsat case _ => 0L }

  def CLOSEANDWATCH(close: ClosingData): Unit
  def ASKREFUNDPEER(some: HasNormalCommits, point: Point): Unit
  def GETREV(cs: NormalCommits, tx: Transaction): Option[RevokedCommitPublished]
  def REV(cs: NormalCommits, rev: RevokeAndAck): Unit

  def doProcess(change: Any) = {
    Tuple3(data, change, state) match {
      case (InitData(announce), cmd: CMDOpenChannel, WAIT_FOR_INIT) =>
        val firstPerCommitPoint = Generators.perCommitPoint(cmd.localParams.shaSeed, index = 0L)
        me SEND OpenChannel(LNParams.chainHash, cmd.tempChanId, cmd.fundingSat, cmd.pushMsat, cmd.localParams.dustLimit.amount,
          cmd.localParams.maxHtlcValueInFlightMsat, cmd.localParams.channelReserveSat, LNParams.minPaymentMsat, cmd.initialFeeratePerKw, cmd.localParams.toSelfDelay,
          cmd.localParams.maxAcceptedHtlcs, cmd.localParams.fundingPrivKey.publicKey, cmd.localParams.revocationBasepoint, cmd.localParams.paymentBasepoint,
          cmd.localParams.delayedPaymentBasepoint, cmd.localParams.htlcBasepoint, firstPerCommitPoint, cmd.channelFlags)

        BECOME(WaitAcceptData(announce, cmd), WAIT_FOR_ACCEPT)


      case (InitData(announce), Tuple2(localParams: LocalParams, open: OpenChannel), WAIT_FOR_INIT) =>
        if (LNParams.chainHash != open.chainHash) throw new LightningException("They have provided a wrong chain hash")
        if (open.channelFlags.isPublic) throw new LightningException("They are offering a public channel and we only support private ones")
        if (open.pushMsat > 1000L * open.fundingSatoshis) throw new LightningException("They are trying to push more than proposed capacity")
        if (open.dustLimitSatoshis > open.channelReserveSatoshis) throw new LightningException("Their dust limit exceeds their channel reserve")
        if (open.feeratePerKw < LNParams.minFeeratePerKw) throw new LightningException("Their proposed opening on-chain fee is too small")
        if (open.toSelfDelay > LNParams.maxToSelfDelay) throw new LightningException("Their toSelfDelay is too high")
        if (open.dustLimitSatoshis < 546L) throw new LightningException("Their on-chain dust limit is too low")
        if (open.maxAcceptedHtlcs > 483) throw new LightningException("They can accept too many payments")
        if (open.pushMsat < 0) throw new LightningException("Their pushMsat is negative")

        val toLocalMsat \ toRemoteMsat = (open.pushMsat, open.fundingSatoshis * 1000L - open.pushMsat)
        if (toLocalMsat <= open.channelReserveSatoshis * 1000L && toRemoteMsat <= open.channelReserveSatoshis * 1000L) throw new LightningException("Both amounts are less than total channel reserve")
        if (open.fundingSatoshis / open.channelReserveSatoshis < LNParams.channelReserveToFundingRatio / 5) throw new LightningException("Their imposed channel reserve is too high relative to capacity")

        val remoteParams = AcceptChannel(open.temporaryChannelId, open.dustLimitSatoshis, open.maxHtlcValueInFlightMsat,
          open.channelReserveSatoshis, open.htlcMinimumMsat, minimumDepth = 6, open.toSelfDelay, open.maxAcceptedHtlcs,
          open.fundingPubkey, open.revocationBasepoint, open.paymentBasepoint, open.delayedPaymentBasepoint,
          open.htlcBasepoint, open.firstPerCommitmentPoint)

        val firstPerCommitPoint = Generators.perCommitPoint(localParams.shaSeed, index = 0L)
        me SEND AcceptChannel(open.temporaryChannelId, localParams.dustLimit.amount, localParams.maxHtlcValueInFlightMsat,
          localParams.channelReserveSat, LNParams.minPaymentMsat, LNParams.minDepth, localParams.toSelfDelay, localParams.maxAcceptedHtlcs,
          localParams.fundingPrivKey.publicKey, localParams.revocationBasepoint, localParams.paymentBasepoint, localParams.delayedPaymentBasepoint,
          localParams.htlcBasepoint, firstPerCommitPoint)

        BECOME(WaitFundingCreatedRemote(announce, localParams, remoteParams, open), WAIT_FOR_ACCEPT)


      case (WaitAcceptData(announce, cmd), accept: AcceptChannel, WAIT_FOR_ACCEPT) if accept.temporaryChannelId == cmd.tempChanId =>
        if (accept.dustLimitSatoshis > cmd.localParams.channelReserveSat) throw new LightningException("Our channel reserve is less than their dust")
        if (UInt64(100000000L) > accept.maxHtlcValueInFlightMsat) throw new LightningException("Their maxHtlcValueInFlightMsat is too low")
        if (accept.channelReserveSatoshis > cmd.fundingSat / 10) throw new LightningException("Their proposed reserve is too high")
        if (accept.toSelfDelay > LNParams.maxToSelfDelay) throw new LightningException("Their toSelfDelay is too high")
        if (accept.dustLimitSatoshis < 546L) throw new LightningException("Their on-chain dust limit is too low")
        if (accept.htlcMinimumMsat > 546000L) throw new LightningException("Their htlcMinimumMsat is too high")
        if (accept.maxAcceptedHtlcs > 483) throw new LightningException("They can accept too many payments")
        if (accept.maxAcceptedHtlcs < 1) throw new LightningException("They can accept too few payments")
        if (accept.minimumDepth > 6L) throw new LightningException("Their minimumDepth is too high")

        val fundTx = cmd.batch.replaceDummyAndSign(cmd.localParams.fundingPrivKey.publicKey, accept.fundingPubkey)
        val (localSpec, localCommitTx, remoteSpec, remoteCommitTx) = Funding.makeFirstCommitTxs(cmd.localParams, cmd.fundingSat,
          cmd.pushMsat, cmd.initialFeeratePerKw, accept, fundTx.hash, cmd.batch.fundOutIdx, accept.firstPerCommitmentPoint)

        val longId = Tools.toLongId(fundTx.hash, cmd.batch.fundOutIdx)
        val localSigOfRemoteTx = Scripts.sign(cmd.localParams.fundingPrivKey)(remoteCommitTx)
        val firstRemoteCommit = RemoteCommit(index = 0L, remoteSpec, Some(remoteCommitTx.tx), accept.firstPerCommitmentPoint)
        val core = WaitFundingSignedCore(cmd.localParams, longId, Some(cmd.channelFlags), accept, localSpec, firstRemoteCommit)
        me SEND FundingCreated(cmd.tempChanId, fundTx.hash, cmd.batch.fundOutIdx, localSigOfRemoteTx)
        BECOME(WaitFundingSignedData(announce, core, localCommitTx, fundTx), WAIT_FUNDING_SIGNED)


      // They have proposed us a channel, we have agreed to their terms and now they have created a funding tx which we should check
      case (WaitFundingCreatedRemote(announce, localParams, accept, open), FundingCreated(_, txid, outIndex, remoteSig), WAIT_FOR_ACCEPT) =>

        val (localSpec, localCommitTx, remoteSpec, remoteCommitTx) =
          Funding.makeFirstCommitTxs(localParams, open.fundingSatoshis, open.pushMsat,
            open.feeratePerKw, accept, txid, outIndex, open.firstPerCommitmentPoint)

        val channelId = Tools.toLongId(txid, outIndex)
        val signedLocalCommitTx = Scripts.addSigs(localCommitTx, localParams.fundingPrivKey.publicKey,
          accept.fundingPubkey, Scripts.sign(localParams.fundingPrivKey)(localCommitTx), remoteSig)

        if (Scripts.checkValid(signedLocalCommitTx).isSuccess) {
          val localSigOfRemoteTx = Scripts.sign(localParams.fundingPrivKey)(remoteCommitTx)
          val rc = RemoteCommit(index = 0L, remoteSpec, Some(remoteCommitTx.tx), open.firstPerCommitmentPoint)
          val core = WaitFundingSignedCore(localParams, channelId, Some(open.channelFlags), accept, localSpec, rc)
          val wait1 = WaitBroadcastRemoteData(announce, core, core makeCommitments signedLocalCommitTx)
          val fundingSigned = FundingSigned(channelId, localSigOfRemoteTx)
          STORESENDBECOME(wait1, WAIT_FUNDING_DONE, fundingSigned)
        } else throw new LightningException


      // LOCAL FUNDER FLOW


      // They have signed our first commit, we can store data and broadcast funding
      case (wait: WaitFundingSignedData, remote: FundingSigned, WAIT_FUNDING_SIGNED) =>
        val localSignature = Scripts.sign(wait.core.localParams.fundingPrivKey)(wait.localCommitTx)
        val localKey \ remoteKey = wait.core.localParams.fundingPrivKey.publicKey -> wait.core.remoteParams.fundingPubkey
        val signedLocalCommitTx = Scripts.addSigs(wait.localCommitTx, localKey, remoteKey, localSignature, remote.signature)
        val wait1 = WaitFundingDoneData(wait.announce, None, None, wait.fundingTx, wait.core makeCommitments signedLocalCommitTx)
        if (Scripts.checkValid(signedLocalCommitTx).isSuccess) BECOME(STORE(wait1), WAIT_FUNDING_DONE) else throw new LightningException


      // BECOMING OPEN


      // We have agreed to proposed incoming channel and they have published a funding tx
      case (wait: WaitBroadcastRemoteData, CMDSpent(fundTx), WAIT_FUNDING_DONE | SLEEPING)
        // GUARD: this is actually a funding tx and we can correctly spend from it
        if fundTxId == fundTx.txid && wait.fundingError.isEmpty =>

        val data1 = decideOnFundeeTx(wait, fundTx)
        val isZeroConfSpendablePush = wait.commitments.channelFlags.exists(_.isZeroConfSpendablePush)
        if (isZeroConfSpendablePush) me process CMDConfirmed(fundTx)
        BECOME(STORE(data1), state)


      // We have agreed to proposed incoming channel and see a confirmed a funding tx right away
      case (wait: WaitBroadcastRemoteData, CMDConfirmed(fundTx), WAIT_FUNDING_DONE | SLEEPING)
        // GUARD: this is actually a funding tx and we can correctly spend from it
        if fundTxId == fundTx.txid && wait.fundingError.isEmpty =>

        val data1 = decideOnFundeeTx(wait, fundTx)
        me process CMDConfirmed(fundTx)
        BECOME(STORE(data1), state)


      case (wait: WaitBroadcastRemoteData, their: FundingLocked, WAIT_FUNDING_DONE) =>
        // For turbo chans, their fundingLocked may arrive faster than onchain event
        // no need to store their fundingLocked because it's re-sent on reconnect
        BECOME(wait.copy(their = their.some), state)


      case (wait: WaitFundingDoneData, theirLocked: FundingLocked, WAIT_FUNDING_DONE) =>
        // Save their `nextPerCommitmentPoint` and maybe become OPEN if have local FundingLocked

        wait.our match {
          case Some(ourFirstFundingLocked) =>
            val cs1 = wait.commitments.copy(remoteNextCommitInfo = theirLocked.nextPointRight)
            STORESENDBECOME(NormalData(wait.announce, cs1), OPEN, ourFirstFundingLocked)

          case None =>
            // Do not store: it gets re-sent on reconnect
            val wait1 = wait.copy(their = theirLocked.some)
            BECOME(wait1, state)
        }


      case (wait: WaitFundingDoneData, CMDConfirmed(fundTx), WAIT_FUNDING_DONE | SLEEPING) if fundTxId == fundTx.txid =>
        // Transaction has been confirmed, save our FundingLocked and maybe become OPEN if we already have their FundingLocked
        val ourFirstFundingLocked = makeFirstFundingLocked(wait)

        wait.their match {
          case Some(theirLocked) =>
            val cs1 = wait.commitments.copy(remoteNextCommitInfo = theirLocked.nextPointRight)
            STORESENDBECOME(NormalData(wait.announce, cs1), OPEN, ourFirstFundingLocked)

          case None =>
            val wait1 = wait.copy(our = ourFirstFundingLocked.some)
            STORESENDBECOME(wait1, state, ourFirstFundingLocked)
        }


      // OPEN MODE


      // GUARD: due to timestamp filter the first update they send must be for our channel
      case (norm: NormalData, upd: ChannelUpdate, OPEN | SLEEPING) if waitingUpdate && !upd.isHosted =>
        if (me isUpdatable upd) data = me STORE norm.modify(_.commitments.updateOpt).setTo(upd.some)
        waitingUpdate = false


      case (norm: NormalData, addHtlc: UpdateAddHtlc, OPEN) =>
        // Got new incoming HTLC, put it to changes without storing for now
        BECOME(norm.copy(commitments = norm.commitments receiveAdd addHtlc), state)


      case (norm: NormalData, fulfill: UpdateFulfillHtlc, OPEN) =>
        // Got a fulfill for an outgoing HTLC we have sent to them earlier
        BECOME(norm.copy(commitments = norm.commitments receiveFulfill fulfill), state)
        events.fulfillReceived(fulfill)


      case (norm: NormalData, fail: UpdateFailHtlc, OPEN) =>
        // Got a failure for an outgoing HTLC we have sent to them earlier
        BECOME(norm.copy(commitments = norm.commitments receiveFail fail), state)


      case (norm: NormalData, fail: UpdateFailMalformedHtlc, OPEN) =>
        // Got 'malformed' failure for an outgoing HTLC we have sent to them earlier
        BECOME(norm.copy(commitments = norm.commitments receiveFailMalformed fail), state)


      case (norm: NormalData, routingData: RoutingData, OPEN) if isOperational(me) =>
        // We can send a new HTLC when channel is both operational and online
        val c1 \ updateAddHtlc = norm.commitments sendAdd routingData
        val norm1 = norm.copy(commitments = c1)
        me SEND updateAddHtlc
        BECOME(norm1, state)


      case (norm: NormalData, CMDFulfillHtlc(preimage, add), OPEN) if pendingIncoming.contains(add) =>
        val fulfillMessage = UpdateFulfillHtlc(norm.commitments.channelId, add.id, preimage)
        val norm1 = norm.modify(_.commitments).using(_ addLocalProposal fulfillMessage)
        me SEND fulfillMessage
        BECOME(norm1, state)


      case (norm: NormalData, CMDFailMalformedHtlc(onionHash, code, add), OPEN) if pendingIncoming.contains(add) =>
        val failMalformedMessage = UpdateFailMalformedHtlc(norm.commitments.channelId, add.id, onionHash, code)
        val norm1 = norm.modify(_.commitments).using(_ addLocalProposal failMalformedMessage)
        me SEND failMalformedMessage
        BECOME(norm1, state)


      case (norm: NormalData, CMDFailHtlc(reason, add), OPEN) if pendingIncoming.contains(add) =>
        val failNormalMessage = UpdateFailHtlc(norm.commitments.channelId, add.id, reason)
        val norm1 = norm.modify(_.commitments).using(_ addLocalProposal failNormalMessage)
        me SEND failNormalMessage
        BECOME(norm1, state)


      case (norm: NormalData, CMDProceed, OPEN)
        // Only if we have a point and something to sign
        if norm.commitments.remoteNextCommitInfo.isRight &&
          (norm.commitments.localChanges.proposed.nonEmpty ||
            norm.commitments.remoteChanges.acked.nonEmpty) =>

        // Propose new remote commit via commit tx sig
        val nextRemotePoint = norm.commitments.remoteNextCommitInfo.right.get
        val c1 \ ourCommitSig = norm.commitments sendCommit nextRemotePoint
        STORESENDBECOME(norm.copy(commitments = c1), state, ourCommitSig)


      case (norm: NormalData, commitSig: CommitSig, OPEN) =>
        // We received a commit sig from them, can update local commit
        val c1 \ ourRevokeAndAck = norm.commitments receiveCommit commitSig
        STORESENDBECOME(norm.copy(commitments = c1), state, ourRevokeAndAck)
        // We may have new proposals to sign
        me process CMDProceed


      case (norm: NormalData, revocation: RevokeAndAck, OPEN) =>
        val cs1 = norm.commitments receiveRevocation revocation
        val norm1 = norm.copy(commitments = cs1)

        BECOME(STORE(norm1), state)
        REV(norm.commitments, revocation)
        events.incomingCrossSigned


      case (norm: NormalData, fee: UpdateFee, OPEN) if !norm.commitments.localParams.isFunder =>
        // It is their duty to update fees when we are a fundee, this will be persisted later
        BECOME(norm.copy(commitments = norm.commitments receiveFee fee), state)


      case (norm: NormalData, CMDFeerate(satPerKw), OPEN) if norm.commitments.localParams.isFunder =>
        val shouldUpdate = LNParams.shouldUpdateFee(satPerKw, norm.commitments.localSpec.feeratePerKw)
        if (shouldUpdate) norm.commitments sendFee satPerKw foreach { case c1 \ funderFeeUpdateMessage =>
          // We send a fee update if current chan unspendable reserve + commitTx fee can afford it
          // otherwise we fail silently in hope that fee will drop or we will receive a payment

          me SEND funderFeeUpdateMessage
          BECOME(norm.copy(commitments = c1), state)
          me process CMDProceed
        }


      // SHUTDOWN in WAIT_FUNDING_DONE


      case (wait: WaitFundingDoneData, CMDShutdown(scriptPubKey), WAIT_FUNDING_DONE) =>
        val finalScriptPubKey = scriptPubKey getOrElse wait.commitments.localParams.defaultFinalScriptPubKey
        val ourShutdown = Shutdown(wait.commitments.channelId, scriptPubKey = finalScriptPubKey)
        val norm = NormalData(wait.announce, wait.commitments, None, ourShutdown.some)
        STORESENDBECOME(norm, OPEN, makeFirstFundingLocked(wait), ourShutdown)


      case (wait: WaitFundingDoneData, CMDShutdown(scriptPubKey), SLEEPING) =>
        val finalScriptPubKey = scriptPubKey getOrElse wait.commitments.localParams.defaultFinalScriptPubKey
        val ourShutdown = Shutdown(wait.commitments.channelId, scriptPubKey = finalScriptPubKey)
        val norm = NormalData(wait.announce, wait.commitments, None, ourShutdown.some)
        BECOME(STORE(norm), SLEEPING)


      case (wait: WaitFundingDoneData, remote: Shutdown, WAIT_FUNDING_DONE) =>
        val ourReplyShutdown = Shutdown(wait.commitments.channelId, wait.commitments.localParams.defaultFinalScriptPubKey)
        val norm = NormalData(wait.announce, wait.commitments, None, ourReplyShutdown.some, remote.some)
        STORESENDBECOME(norm, OPEN, ourReplyShutdown)
        me process CMDProceed


      // SHUTDOWN in OPEN


      case (norm @ NormalData(announce, commitments, txOpt, our, their), CMDShutdown(script), OPEN | SLEEPING) =>
        if (commitments.localHasUnsignedOutgoing || our.isDefined || their.isDefined) startLocalClose(norm) else {
          val ourShutdown = Shutdown(commitments.channelId, script getOrElse commitments.localParams.defaultFinalScriptPubKey)
          val norm1 = NormalData(announce, commitments, txOpt, ourShutdown.some, their)
          STORESENDBECOME(norm1, state, ourShutdown)
        }


      // Either they initiate a shutdown or respond to the one we have sent
      case (NormalData(announce, commitments, txOpt, our, None), remote: Shutdown, OPEN) =>
        val norm1 = NormalData(announce, commitments, txOpt, our, remoteShutdown = remote.some)
        if (commitments.remoteHasUnsignedOutgoing) startLocalClose(norm1) else BECOME(norm1, state)
        // Should sign our unsigned outgoing HTLCs if present and then proceed with shutdown
        me process CMDProceed


      case (NormalData(announce, commitments, txOpt, our, their), CMDProceed, OPEN)
        // GUARD: we have nothing to sign and no payments in-flight so check if maybe closing can start
        if commitments.reducedRemoteSpec.htlcs.isEmpty && commitments.localCommit.spec.htlcs.isEmpty =>

        (our, their) match {
          case Some(ourSig) \ Some(theirSig) =>
            if (commitments.localParams.isFunder) {
              // Got both shutdowns without HTLCs in-flight so can send a first closing since we are the funder
              val firstProposed = Closing.makeFirstClosing(commitments, ourSig.scriptPubKey, theirSig.scriptPubKey)
              val neg = NegotiationsData(announce, commitments, ourSig, theirSig, firstProposed :: Nil)
              STORESENDBECOME(neg, NEGOTIATIONS, firstProposed.localClosingSigned)
            } else {
              // Got both shutdowns without HTLCs in-flight so wait for funder's proposal
              val neg = NegotiationsData(announce, commitments, ourSig, theirSig, Nil)
              BECOME(STORE(neg), NEGOTIATIONS)
            }

          case None \ Some(theirSig) =>
            // We have previously received their Shutdown so can respond, then CMDProceed to enter NEGOTIATIONS
            val ourShutdown = Shutdown(commitments.channelId, commitments.localParams.defaultFinalScriptPubKey)
            val norm1 = NormalData(announce, commitments, txOpt, ourShutdown.some, theirSig.some)
            STORESENDBECOME(norm1, state, ourShutdown)
            me process CMDProceed

          case Some(ourSig) \ None =>
            // Was initiated in SLEEPING
            me SEND ourSig

          // Not ready
          case _ =>
        }


      // SYNC and REFUNDING MODE


      case (ref: RefundingData, cr: ChannelReestablish, REFUNDING) =>
        cr.myCurrentPerCommitmentPoint -> ref.remoteLatestPoint match {
          case _ \ Some(ourSavedPoint) => ASKREFUNDPEER(ref, ourSavedPoint)
          case Some(theirPoint) \ _ => ASKREFUNDPEER(ref, theirPoint)
          case _ => throw new LightningException("No remote point")
        }


      case (some: HasNormalCommits, cr: ChannelReestablish, SLEEPING)
        // GUARD: their nextRemoteRevocationNumber is unexpectedly too far away
        if some.commitments.localCommit.index < cr.nextRemoteRevocationNumber && cr.myCurrentPerCommitmentPoint.isDefined =>
        val secret = Generators.perCommitSecret(some.commitments.localParams.shaSeed, cr.nextRemoteRevocationNumber - 1)
        if (cr.yourLastPerCommitmentSecret contains secret) ASKREFUNDPEER(some, cr.myCurrentPerCommitmentPoint.get)
        else throw new LightningException("Peer can not prove having a more recent state")


      case (norm: NormalData, cr: ChannelReestablish, SLEEPING) =>
        // Resend our FundingLocked if a channel is fresh since they might not receive it before reconnect
        val reSendFundingLocked = cr.nextLocalCommitmentNumber == 1 && norm.commitments.localCommit.index == 0
        if (reSendFundingLocked) me SEND makeFirstFundingLocked(norm)

        // First we clean up unacknowledged updates
        val localDelta = norm.commitments.localChanges.proposed collect { case _: UpdateAddHtlc => true }
        val remoteDelta = norm.commitments.remoteChanges.proposed collect { case _: UpdateAddHtlc => true }
        val cs1 = norm.commitments.modifyAll(_.localChanges.proposed, _.remoteChanges.proposed).setTo(Vector.empty)
          .modify(_.remoteNextHtlcId).using(_ - remoteDelta.size).modify(_.localNextHtlcId).using(_ - localDelta.size)

        def maybeResendRevocationMessage = if (cs1.localCommit.index == cr.nextRemoteRevocationNumber + 1) {
          val localPerCommitmentSecret = Generators.perCommitSecret(cs1.localParams.shaSeed, cs1.localCommit.index - 1)
          val localNextPerCommitmentPoint = Generators.perCommitPoint(cs1.localParams.shaSeed, cs1.localCommit.index + 1)
          me SEND RevokeAndAck(channelId = cs1.channelId, localPerCommitmentSecret, localNextPerCommitmentPoint)
        } else if (cs1.localCommit.index != cr.nextRemoteRevocationNumber) throw new LightningException

        cs1.remoteNextCommitInfo match {
          // We had sent a new sig and were waiting for their revocation
          // they didn't receive the new sig because disconnection happened
          // we resend the same updates and sig, also be careful about revocation
          case Left(waitForRevocation) if waitForRevocation.nextRemoteCommit.index == cr.nextLocalCommitmentNumber =>
            val revocationWasSentLast = cs1.localCommit.index > waitForRevocation.localCommitIndexSnapshot

            if (!revocationWasSentLast) maybeResendRevocationMessage
            SEND(cs1.localChanges.signed :+ waitForRevocation.sent:_*)
            if (revocationWasSentLast) maybeResendRevocationMessage

          // We had sent a new sig and were waiting for their revocation, they had received
          // the new sig but their revocation was lost during the disconnection, they'll resend us the revocation
          case Left(wait) if wait.nextRemoteCommit.index + 1 == cr.nextLocalCommitmentNumber => maybeResendRevocationMessage
          case Right(_) if cs1.remoteCommit.index + 1 == cr.nextLocalCommitmentNumber => maybeResendRevocationMessage
          case _ => throw new LightningException("Local sync error")
        }

        // CMDProceed is on ChannelManager
        BECOME(norm.copy(commitments = cs1), OPEN)


      // We're exiting a sync state while funding tx is still not provided
      case (remote: WaitBroadcastRemoteData, _: ChannelReestablish, SLEEPING) =>
        // Need to check whether a funding is present in a listener
        BECOME(remote, WAIT_FUNDING_DONE)


      case (wait: WaitFundingDoneData, _: ChannelReestablish, SLEEPING) =>
        // We're exiting a sync state while waiting for their FundingLocked

        SEND(wait.our.toVector:_*)
        BECOME(wait, WAIT_FUNDING_DONE)


      // No in-flight HTLCs here, just proceed with negotiations
      case (negs: NegotiationsData, _: ChannelReestablish, SLEEPING) =>
        // Last closing signed may be empty if we are not a funder of this channel
        val lastClosingSignedOpt = negs.localProposals.headOption.map(_.localClosingSigned)
        SEND(negs.localShutdown +: lastClosingSignedOpt.toVector:_*)
        BECOME(negs, NEGOTIATIONS)


      case (some: HasNormalCommits, CMDSocketOnline, SLEEPING) =>
        // According to BOLT a first message on connection should be reestablish
        // will specifically NOT work in REFUNDING to not let them know beforehand
        me SEND makeReestablish(some, some.commitments.localCommit.index + 1)
        permanentOffline = false


      case (some: HasNormalCommits, newAnn: NodeAnnouncement, NEGOTIATIONS | SLEEPING | REFUNDING)
        // Node has been unconnected for a long time, update but do not trigger listeners
        if some.announce.nodeId == newAnn.nodeId && Announcements.checkSig(newAnn) =>
        data = me STORE some.modify(_.announce).setTo(newAnn)


      case (norm: NormalData, CMDChainTipKnown, _) if norm.commitments.revealedPreimages.toMap.values.exists(ChannelManager.cltvExpiryCloseToExpiration) =>
        // We have an incoming payment for which we have revealed a preimage and yet that payment has been pending for a long time (peer offline or not responding)
        // it got dangerously close to CLTV expiration now so we need to break a channel and fetch it on-chain while avoiding a race condition
        startLocalClose(norm)


      case (wait: WaitBroadcastRemoteData, CMDSocketOffline, WAIT_FUNDING_DONE) => BECOME(wait, SLEEPING)
      case (wait: WaitFundingDoneData, CMDSocketOffline, WAIT_FUNDING_DONE) => BECOME(wait, SLEEPING)
      case (negs: NegotiationsData, CMDSocketOffline, NEGOTIATIONS) => BECOME(negs, SLEEPING)
      case (norm: NormalData, CMDSocketOffline, OPEN) => BECOME(norm, SLEEPING)


      // NEGOTIATIONS MODE


      case (neg @ NegotiationsData(_, commitments, localShutdown, remoteShutdown, localProposals, _), ClosingSigned(_, remoteFee, remoteClosingSig), NEGOTIATIONS) =>
        val ClosingTxProposed(closing, closingSigned) = Closing.makeClosing(commitments, closingFee = Satoshi(remoteFee), localShutdown.scriptPubKey, remoteShutdown.scriptPubKey)
        val signedClose = Scripts.addSigs(closing, commitments.localParams.fundingPrivKey.publicKey, commitments.remoteParams.fundingPubkey, closingSigned.signature, remoteClosingSig)

        val lastLocalFee = localProposals.headOption.map(_.localClosingSigned.feeSatoshis) getOrElse {
          // If we are fundee and we were waiting for them to send their first proposal, we don't have a lastLocalClosingFee
          val closing = Closing.makeFirstClosing(commitments, localShutdown.scriptPubKey, remoteShutdown.scriptPubKey)
          closing.localClosingSigned.feeSatoshis
        }

        Scripts.checkValid(txWithInputInfo = signedClose) match {
          // Our current and their proposed fees are equal for this tx
          case Success(okClose) if remoteFee == lastLocalFee =>
            startMutualClose(neg, okClose.tx)

          case Success(okClose) =>
            val fee1 = Satoshi(lastLocalFee + remoteFee) / 4 * 2
            val nextProposed = Closing.makeClosing(commitments, fee1, localShutdown.scriptPubKey, remoteShutdown.scriptPubKey)
            val neg1 = neg.copy(lastSignedTx = Some(okClose), localProposals = nextProposed +: neg.localProposals)
            if (remoteFee == fee1.amount) startMutualClose(neg, okClose.tx) else BECOME(STORE(neg1), state)
            me SEND nextProposed.localClosingSigned

          case _ =>
            startLocalClose(neg)
            // Nothing left to do here so at least inform user and force-close
            throw new LightningException("Remote shutdown signature check failed")
        }


      // Disregard custom scriptPubKey and always refund to local wallet
      case (negs: NegotiationsData, _: CMDShutdown, NEGOTIATIONS | SLEEPING) => startLocalClose(negs)
      case (_, routingData: RoutingData, _) => throw CMDAddImpossible(routingData, ERR_CAN_NOT_ADD)


      // HANDLE FUNDING SPENT


      case (RefundingData(announce, Some(remoteLatestPoint), commitments), CMDSpent(spendTx), REFUNDING)
        // GUARD: we have got a remote commit which we asked them to spend and we have their point
        if spendTx.txIn.exists(input => commitments.commitInput.outPoint == input.outPoint) =>
        val rcp = Closing.claimRemoteMainOutput(commitments, remoteLatestPoint, spendTx)
        val d1 = ClosingData(announce, commitments, refundRemoteCommit = rcp :: Nil)
        BECOME(STORE(d1), CLOSING)


      case (cd: ClosingData, cmd: CMDFulfillHtlc, CLOSING) =>
        val nextRemoteCommitOpt = cd.commitments.nextRemoteCommitOpt
        val fulfill = UpdateFulfillHtlc(cd.commitments.channelId, cmd.add.id, cmd.preimage)
        val publishUpdatedClosingData = (cd.modify(_.commitments).using(_ addLocalProposal fulfill).copy(localCommit = Nil, remoteCommit = Nil) /: cd.commitTxs) {
          case cd1 \ tx if nextRemoteCommitOpt.flatMap(_.txOpt).exists(_.txid == tx.txid) => cd1.modify(_.remoteCommit).using(Closing.claimRemoteCommitTxOutputs(cd1.commitments, nextRemoteCommitOpt.get) +: _)
          case cd1 \ tx if cd1.commitments.remoteCommit.txOpt.exists(_.txid == tx.txid) => cd1.modify(_.remoteCommit).using(Closing.claimRemoteCommitTxOutputs(cd1.commitments, cd1.commitments.remoteCommit) +: _)
          case cd1 \ tx if cd1.commitments.localCommit.commitTx.tx.txid == tx.txid => cd1.copy(localCommit = Closing.claimCurrentLocalCommitTxOutputs(cd1.commitments) :: Nil)
          case cd1 \ _ => cd1
        }

        BECOME(STORE(publishUpdatedClosingData), state)
        // Send a specific command to trigger publishing
        me process CMDChainTipKnown


      // This will make incoming HTLC in a CLOSING channel invisible to `pendingIncoming` method which is desired
      case (cd: ClosingData, CMDFailMalformedHtlc(onionHash, code, add), CLOSING) if pendingIncoming.contains(add) =>
        val failMalformedMessage = UpdateFailMalformedHtlc(cd.commitments.channelId, add.id, onionHash, code)
        val cd1 = cd.modify(_.commitments).using(_ addLocalProposal failMalformedMessage)
        BECOME(STORE(cd1), state)


      case (cd: ClosingData, CMDFailHtlc(reason, add), CLOSING) if pendingIncoming.contains(add) =>
        val failNormalMessage = UpdateFailHtlc(cd.commitments.channelId, add.id, reason)
        val cd1 = cd.modify(_.commitments).using(_ addLocalProposal failNormalMessage)
        BECOME(STORE(cd1), state)


      case (cd: ClosingData, CMDSpent(htlcTx), CLOSING)
        // This may be one of our own 1st tier transactions
        // or they may broadcast their 1st tier, catch all of them
        if cd.revokedCommit.exists(_ spendsFromRevoked htlcTx) =>

        for {
          revCommitPublished <- cd.revokedCommit if revCommitPublished spendsFromRevoked htlcTx
          perCommitmentSecret <- Helpers.Closing.extractCommitmentSecret(cd.commitments, revCommitPublished.commitTx)
          punishTx <- Closing.claimRevokedHtlcTxOutputs(commitments = cd.commitments, htlcTx, perCommitmentSecret)
          punishTxj = com.lightning.walletapp.lnutils.ImplicitConversions.bitcoinLibTx2bitcoinjTx(punishTx)
        } com.lightning.walletapp.Utils.app.kit blockSend punishTxj


      case (some: HasNormalCommits, CMDSpent(tx), currentState)
        // GUARD: tx which spends our funding is broadcasted, must react
        if tx.txIn.exists(input => some.commitments.commitInput.outPoint == input.outPoint) =>
        Tuple3(GETREV(some.commitments, tx), some.commitments.nextRemoteCommitOpt, some) match {
          case (_, _, close: ClosingData) if close.refundRemoteCommit.nonEmpty => Tools log s"Existing refund $tx"
          case (_, _, close: ClosingData) if close.mutualClose.exists(_.txid == tx.txid) => Tools log s"Existing mutual $tx"
          case (_, _, close: ClosingData) if close.localCommit.exists(_.commitTx.txid == tx.txid) => Tools log s"Existing local $tx"
          case (_, _, close: ClosingData) if close.localProposals.exists(_.unsignedTx.tx.txid == tx.txid) => startMutualClose(close, tx)
          case (_, _, negs: NegotiationsData) if negs.localProposals.exists(_.unsignedTx.tx.txid == tx.txid) => startMutualClose(negs, tx)
          case (Some(revokingClaim), _, closingData: ClosingData) => me CLOSEANDWATCH closingData.modify(_.revokedCommit).using(revokingClaim +: _)
          case (Some(revokingClaim), _, _) => me CLOSEANDWATCH ClosingData(some.announce, some.commitments, revokedCommit = revokingClaim :: Nil)
          case (_, Some(nextRemoteCommit), _) if nextRemoteCommit.txOpt.exists(_.txid == tx.txid) => startRemoteClose(some, nextRemoteCommit)
          case _ if some.commitments.remoteCommit.txOpt.exists(_.txid == tx.txid) => startRemoteClose(some, some.commitments.remoteCommit)
          case _ if some.commitments.localCommit.commitTx.tx.txid == tx.txid => startLocalClose(some)

          case (_, _, norm: NormalData) =>
            // Example: old snapshot is used two times in a row
            val d1 = norm.modify(_.unknownSpend) setTo Some(tx)
            BECOME(STORE(d1), currentState)

          case _ =>
            // Nothing left to do here so at least inform user
            throw new LightningException("Unknown spend detected")
        }


      // HANDLE INITIALIZATION


      case (null, ref: RefundingData, null) => super.become(ref, REFUNDING)
      case (null, close: ClosingData, null) => super.become(close, CLOSING)
      case (null, init: InitData, null) => super.become(init, WAIT_FOR_INIT)
      case (null, wait: WaitFundingDoneData, null) => super.become(wait, SLEEPING)
      case (null, wait: WaitBroadcastRemoteData, null) => super.become(wait, SLEEPING)
      case (null, negs: NegotiationsData, null) => super.become(negs, SLEEPING)
      case (null, norm: NormalData, null) => super.become(norm, SLEEPING)
      case _ =>
    }

    // Change has been processed without failures
    events onProcessSuccess Tuple3(me, data, change)
  }

  def makeReestablish(some: HasNormalCommits, nextLocalCommitmentNumber: Long) = {
    val ShaHashesWithIndex(hashes, lastIndex) = some.commitments.remotePerCommitmentSecrets
    val yourLastPerCommitmentSecret = lastIndex.map(ShaChain.moves).flatMap(ShaChain getHash hashes).map(ByteVector.view)
    val myCurrentPerCommitmentPoint = Generators.perCommitPoint(some.commitments.localParams.shaSeed, some.commitments.localCommit.index)
    val myCurrentPerCommitmentPointOpt = Some(myCurrentPerCommitmentPoint)

    ChannelReestablish(some.commitments.channelId, nextLocalCommitmentNumber, some.commitments.remoteCommit.index,
      yourLastPerCommitmentSecret.map(Scalar.apply) orElse Some(Zeroes), myCurrentPerCommitmentPointOpt)
  }

  def makeFirstFundingLocked(some: HasNormalCommits) = {
    val first = Generators.perCommitPoint(some.commitments.localParams.shaSeed, 1L)
    FundingLocked(some.commitments.channelId, nextPerCommitmentPoint = first)
  }

  def decideOnFundeeTx(wait: WaitBroadcastRemoteData, fundTx: Transaction) = Try {
    val ourFirstCommitmentTransaction: Transaction = wait.commitments.localCommit.commitTx.tx
    Transaction.correctlySpends(ourFirstCommitmentTransaction, Seq(fundTx), STANDARD_SCRIPT_VERIFY_FLAGS)
  } match {
    case Failure(reason) => wait.modify(_.fundingError) setTo Some(reason.getMessage)
    case _ => WaitFundingDoneData(wait.announce, None, wait.their, fundTx, wait.commitments)
  }

  private def startMutualClose(some: HasNormalCommits, tx: Transaction) = some match {
    case closingData: ClosingData => BECOME(me STORE closingData.copy(mutualClose = tx +: closingData.mutualClose), CLOSING)
    case neg: NegotiationsData => BECOME(me STORE ClosingData(neg.announce, neg.commitments, neg.localProposals, tx :: Nil), CLOSING)
    case _ => BECOME(me STORE ClosingData(some.announce, some.commitments, Nil, tx :: Nil), CLOSING)
  }

  def startLocalClose(some: HasNormalCommits): Unit =
    // Something went wrong and we decided to spend our CURRENT commit transaction
    Tuple2(Closing.claimCurrentLocalCommitTxOutputs(some.commitments), some) match {
      case (_, neg: NegotiationsData) if neg.lastSignedTx.isDefined => startMutualClose(neg, neg.lastSignedTx.get.tx)
      case (localClaim, closingData: ClosingData) => me CLOSEANDWATCH closingData.copy(localCommit = localClaim :: Nil)
      case (localClaim, _) => me CLOSEANDWATCH ClosingData(some.announce, some.commitments, localCommit = localClaim :: Nil)
    }

  // They've decided to spend their CURRENT or NEXT commit tx
  private def startRemoteClose(some: HasNormalCommits, remoteCommit: RemoteCommit) =
    Tuple2(Closing.claimRemoteCommitTxOutputs(some.commitments, remoteCommit), some) match {
      case (remoteClaim, closingData: ClosingData) => me CLOSEANDWATCH closingData.modify(_.remoteCommit).using(remoteClaim +: _)
      case (remoteClaim, _) => me CLOSEANDWATCH ClosingData(some.announce, some.commitments, remoteCommit = remoteClaim :: Nil)
    }
}

abstract class HostedChannel extends Channel(isHosted = true) { me =>
  def isBlockDayOutOfSync(blockDay: Long): Boolean = math.abs(blockDay - ChannelManager.currentBlockDay) > 1
  def pendingOutgoing = data match { case hc: HostedCommits => hc.localSpec.outgoingAdds ++ hc.nextLocalSpec.outgoingAdds case _ => Set.empty }
  def pendingIncoming = data match { case hc: HostedCommits => hc.localSpec.incomingAdds intersect hc.nextLocalSpec.incomingAdds case _ => Set.empty }
  def remoteUsableMsat = data match { case hc: HostedCommits => hc.nextLocalSpec.toRemoteMsat case _ => 0L }
  def localUsableMsat = data match { case hc: HostedCommits => hc.nextLocalSpec.toLocalMsat case _ => 0L }
  def refundableMsat = localUsableMsat

  private var isChainHeightKnown: Boolean = false
  private var isSocketConnected: Boolean = false

  def doProcess(change: Any) = {
    Tuple3(data, change, state) match {
      case (wait @ WaitRemoteHostedReply(_, refundScriptPubKey, secret), CMDSocketOnline, WAIT_FOR_INIT) =>
        if (isChainHeightKnown) me SEND InvokeHostedChannel(LNParams.chainHash, refundScriptPubKey, secret)
        if (isChainHeightKnown) BECOME(wait, WAIT_FOR_ACCEPT)
        isSocketConnected = true
        permanentOffline = false


      case (wait @ WaitRemoteHostedReply(_, refundScriptPubKey, secret), CMDChainTipKnown, WAIT_FOR_INIT) =>
        if (isSocketConnected) me SEND InvokeHostedChannel(LNParams.chainHash, refundScriptPubKey, secret)
        if (isSocketConnected) BECOME(wait, WAIT_FOR_ACCEPT)
        isChainHeightKnown = true


      case (WaitRemoteHostedReply(announce, refundScriptPubKey, _), init: InitHostedChannel, WAIT_FOR_ACCEPT) =>
        require(Features.areSupported(Set.empty, init.features.bits.reverse), "Unsupported features found, you should probably update an app")
        if (init.liabilityDeadlineBlockdays < LNParams.minHostedLiabilityBlockdays) throw new LightningException("Their liability deadline is too low")
        if (init.initialClientBalanceMsat > init.channelCapacityMsat) throw new LightningException("Their init balance for us is larger than capacity")
        if (init.minimalOnchainRefundAmountSatoshis > LNParams.minHostedOnChainRefundSat) throw new LightningException("Their min refund is too high")
        if (init.channelCapacityMsat < LNParams.minHostedOnChainRefundSat) throw new LightningException("Their proposed channel capacity is too low")
        if (UInt64(100000000L) > init.maxHtlcValueInFlightMsat) throw new LightningException("Their max value in-flight is too low")
        if (init.htlcMinimumMsat > 546000L) throw new LightningException("Their minimal payment size is too high")
        if (init.maxAcceptedHtlcs < 1) throw new LightningException("They can accept too few payments")

        val localHalfSignedHC =
          restoreCommits(LastCrossSignedState(refundScriptPubKey, init, ChannelManager.currentBlockDay, init.initialClientBalanceMsat,
            init.channelCapacityMsat - init.initialClientBalanceMsat, localUpdates = 0L, remoteUpdates = 0L, incomingHtlcs = Nil, outgoingHtlcs = Nil,
            localSigOfRemote = ByteVector.empty, remoteSigOfLocal = ByteVector.empty).withLocalSigOfRemote(LNParams.keys.extendedNodeKey.privateKey), announce)

        me SEND localHalfSignedHC.lastCrossSignedState.stateUpdate(isTerminal = true)
        BECOME(WaitRemoteHostedStateUpdate(announce, localHalfSignedHC), state)


      case (WaitRemoteHostedStateUpdate(announce, hc), remoteSU: StateUpdate, WAIT_FOR_ACCEPT) =>
        val localCompleteLCSS = hc.lastCrossSignedState.copy(remoteSigOfLocal = remoteSU.localSigOfRemoteLCSS)
        val isRightRemoteUpdateNumber = hc.lastCrossSignedState.remoteUpdates == remoteSU.localUpdates
        val isRightLocalUpdateNumber = hc.lastCrossSignedState.localUpdates == remoteSU.remoteUpdates
        val isRemoteSigOk = localCompleteLCSS.verifyRemoteSig(hc.announce.nodeId)

        if (me isBlockDayOutOfSync remoteSU.blockDay) throw new LightningException("Their blockday is wrong")
        if (!isRightRemoteUpdateNumber) throw new LightningException("Their remote update number is wrong")
        if (!isRightLocalUpdateNumber) throw new LightningException("Their local update number is wrong")
        if (!isRemoteSigOk) throw new LightningException("Their signature is wrong")
        val hc1 = hc.copy(lastCrossSignedState = localCompleteLCSS)
        // CMDProceed is on ChannelManager
        BECOME(STORE(hc1), OPEN)


      case (wait: WaitRemoteHostedReply, remoteLCSS: LastCrossSignedState, WAIT_FOR_ACCEPT) =>
        // We have expected InitHostedChannel but got LastCrossSignedState so this channel exists already
        // make sure our signature match and if so then become OPEN using host supplied state data
        val isLocalSigOk = remoteLCSS.verifyRemoteSig(LNParams.keys.extendedNodeKey.publicKey)
        val isRemoteSigOk = remoteLCSS.reverse.verifyRemoteSig(wait.announce.nodeId)
        val hc = restoreCommits(remoteLCSS.reverse, wait.announce)

        if (!isRemoteSigOk) localSuspend(hc, ERR_HOSTED_WRONG_REMOTE_SIG)
        else if (!isLocalSigOk) localSuspend(hc, ERR_HOSTED_WRONG_LOCAL_SIG)
        else STORESENDBECOME(hc, OPEN, hc.lastCrossSignedState)


      // CHANNEL IS ESTABLISHED


      case (hc: HostedCommits, addHtlc: UpdateAddHtlc, OPEN) =>
        // They have sent us an incoming payment, do not store yet
        BECOME(hc.receiveAdd(addHtlc), state)


      // Process their fulfill in any state to make sure we always get a preimage
      // fails/fulfills when SUSPENDED are ignored because they may fulfill afterwards
      case (hc: HostedCommits, fulfill: UpdateFulfillHtlc, SLEEPING | OPEN | SUSPENDED) =>
        // Technically peer may send a preimage any time, even if new LCSS has not been reached yet so do our best and always resolve on getting it
        val isPresent = hc.nextLocalSpec.outgoingAdds.exists(add => add.id == fulfill.id && add.paymentHash == fulfill.paymentHash)
        if (isPresent) BECOME(hc.addProposal(fulfill.remote), state) else throw new LightningException
        events.fulfillReceived(fulfill)


      case (hc: HostedCommits, fail: UpdateFailHtlc, OPEN) =>
        // For both types of Fail we only consider them when channel is OPEN and only accept them if our outging payment has not been resolved already
        val isNotResolvedYet = hc.localSpec.findHtlcById(fail.id, isIncoming = false).isDefined && hc.nextLocalSpec.findHtlcById(fail.id, isIncoming = false).isDefined
        if (isNotResolvedYet) BECOME(hc.addProposal(fail.remote), state) else throw new LightningException("Peer failed an HTLC which is either not cross-signed or does not exist")


      case (hc: HostedCommits, fail: UpdateFailMalformedHtlc, OPEN) =>
        if (fail.failureCode.&(FailureMessageCodecs.BADONION) == 0) throw new LightningException("Wrong failure code for malformed onion")
        val isNotResolvedYet = hc.localSpec.findHtlcById(fail.id, isIncoming = false).isDefined && hc.nextLocalSpec.findHtlcById(fail.id, isIncoming = false).isDefined
        if (isNotResolvedYet) BECOME(hc.addProposal(fail.remote), state) else throw new LightningException("Peer malformed-failed an HTLC which is either not cross-signed or does not exist")


      case (hc: HostedCommits, routingData: RoutingData, OPEN) =>
        val hc1 \ updateAddHtlc = hc.sendAdd(routingData)
        me SEND updateAddHtlc
        BECOME(hc1, state)


      case (hc: HostedCommits, CMDProceed, OPEN) if hc.futureUpdates.nonEmpty =>
        val nextHC = hc.nextLocalUnsignedLCSS(ChannelManager.currentBlockDay).withLocalSigOfRemote(LNParams.keys.extendedNodeKey.privateKey)
        // Tell them not to resolve updates yet even though they may have a cross-signed state because they may also be sending changes
        me SEND nextHC.stateUpdate(isTerminal = false)


      case (hc: HostedCommits, StateUpdate(blockDay, _, remoteUpdates, localSigOfRemoteLCSS, isTerminal), OPEN)
        // GUARD: only proceed if signture is defferent because they will send a few non-terminal duplicates
        if hc.lastCrossSignedState.remoteSigOfLocal != localSigOfRemoteLCSS =>

        val lcss1 = hc.nextLocalUnsignedLCSS(blockDay).copy(remoteSigOfLocal = localSigOfRemoteLCSS).withLocalSigOfRemote(LNParams.keys.extendedNodeKey.privateKey)
        val hc1 = hc.copy(lastCrossSignedState = lcss1, localSpec = hc.nextLocalSpec, futureUpdates = Vector.empty)
        val isRemoteSigOk = lcss1.verifyRemoteSig(hc.announce.nodeId)

        if (remoteUpdates < lcss1.localUpdates) me SEND lcss1.stateUpdate(isTerminal = false)
        else if (me isBlockDayOutOfSync blockDay) localSuspend(hc, ERR_HOSTED_WRONG_BLOCKDAY)
        else if (!isRemoteSigOk) localSuspend(hc, ERR_HOSTED_WRONG_REMOTE_SIG)
        else if (!isTerminal) me SEND lcss1.stateUpdate(isTerminal = true)
        else {
          val nextLocalSU = lcss1.stateUpdate(isTerminal = true)
          STORESENDBECOME(hc1, state, nextLocalSU)
          events.incomingCrossSigned
        }


      // Normal channel fetches on-chain when CLOSED, hosted sends a preimage and signals on UI when SUSPENDED
      case (hc: HostedCommits, CMDFulfillHtlc(preimage, add), OPEN | SUSPENDED) if pendingIncoming.contains(add) =>
        val updateFulfill = UpdateFulfillHtlc(hc.channelId, add.id, paymentPreimage = preimage)
        STORESENDBECOME(hc.addProposal(updateFulfill.local), state, updateFulfill)

      // This will make pending incoming HTLC in a SUSPENDED channel invisible to `pendingIncoming` method which is desired
      case (hc: HostedCommits, CMDFailMalformedHtlc(onionHash, code, add), OPEN | SUSPENDED) if pendingIncoming.contains(add) =>
        val updateFailMalformed = UpdateFailMalformedHtlc(hc.channelId, add.id, onionHash, failureCode = code)
        STORESENDBECOME(hc.addProposal(updateFailMalformed.local), state, updateFailMalformed)


      case (hc: HostedCommits, CMDFailHtlc(encoded, add), OPEN | SUSPENDED) if pendingIncoming.contains(add) =>
        val updateFail = UpdateFailHtlc(hc.channelId, add.id, reason = encoded)
        STORESENDBECOME(hc.addProposal(updateFail.local), state, updateFail)


      case (hc: HostedCommits, CMDSocketOnline, SLEEPING | SUSPENDED) =>
        if (isChainHeightKnown) me SEND hc.getError.getOrElse(hc.invokeMsg)
        isSocketConnected = true
        permanentOffline = false


      case (hc: HostedCommits, CMDChainTipKnown, SLEEPING | SUSPENDED) =>
        if (isSocketConnected) me SEND hc.getError.getOrElse(hc.invokeMsg)
        isChainHeightKnown = true


      case (hc: HostedCommits, CMDSocketOffline, OPEN) =>
        isSocketConnected = false
        BECOME(hc, SLEEPING)


      case (hc: HostedCommits, init: InitHostedChannel, SLEEPING) =>
        // Remote peer has lost this channel, they should re-sync from our LCSS
        syncAndResend(hc, hc.futureUpdates, hc.lastCrossSignedState, hc.localSpec)


      // Technically they can send a remote LCSS before us explicitly asking for it first
      // this may be a problem if chain height is not yet known and we have incoming HTLCs to resolve
      case (hc: HostedCommits, remoteLCSS: LastCrossSignedState, SLEEPING) if isChainHeightKnown =>
        val isLocalSigOk = remoteLCSS.verifyRemoteSig(LNParams.keys.extendedNodeKey.publicKey)
        val isRemoteSigOk = remoteLCSS.reverse.verifyRemoteSig(hc.announce.nodeId)
        val weAreAhead = hc.lastCrossSignedState.isAhead(remoteLCSS)
        val weAreEven = hc.lastCrossSignedState.isEven(remoteLCSS)

        if (!isRemoteSigOk) localSuspend(hc, ERR_HOSTED_WRONG_REMOTE_SIG)
        else if (!isLocalSigOk) localSuspend(hc, ERR_HOSTED_WRONG_LOCAL_SIG)
        // They have our current or previous state, resend all our future updates and keep the current state
        else if (weAreAhead || weAreEven) syncAndResend(hc, hc.futureUpdates, hc.lastCrossSignedState, hc.localSpec)
        else hc.findState(remoteLCSS).headOption match {

          case Some(hc1) =>
            val leftOvers = hc.futureUpdates.diff(hc1.futureUpdates)
            // They have our future state, settle on it and resend local leftovers
            syncAndResend(hc1, leftOvers, remoteLCSS.reverse, hc1.nextLocalSpec)

          case None =>
            // We are far behind, restore state and resolve incoming adds
            val hc1 = restoreCommits(remoteLCSS.reverse, hc.announce)
            // HTLC resolution and CMDProceed is on ChannelManager
            STORESENDBECOME(hc1, OPEN, hc1.lastCrossSignedState)
        }


      case (hc: HostedCommits, newAnn: NodeAnnouncement, SLEEPING | SUSPENDED)
        if hc.announce.nodeId == newAnn.nodeId && Announcements.checkSig(newAnn) =>
        data = me STORE hc.copy(announce = newAnn)


      case (hc: HostedCommits, upd: ChannelUpdate, OPEN | SLEEPING) if waitingUpdate && upd.isHosted =>
        if (me isUpdatable upd) data = me STORE hc.copy(updateOpt = upd.some)
        waitingUpdate = false


      case (hc: HostedCommits, remoteError: Error, WAIT_FOR_ACCEPT | OPEN | SLEEPING) =>
        val hc1 = hc.modify(_.remoteError) setTo Some(remoteError)
        BECOME(STORE(hc1), SUSPENDED)


      // User has accepted a proposed remote override, now make sure all provided parameters check out
      case (hc: HostedCommits, CMDHostedStateOverride(remoteOverride), SUSPENDED) if isSocketConnected =>
        val localBalance = hc.newLocalBalanceMsat(remoteOverride)

        val restoredLCSS =
          hc.lastCrossSignedState.copy(incomingHtlcs = Nil, outgoingHtlcs = Nil,
            localBalanceMsat = localBalance, remoteBalanceMsat = remoteOverride.localBalanceMsat,
            localUpdates = remoteOverride.remoteUpdates, remoteUpdates = remoteOverride.localUpdates,
            blockDay = remoteOverride.blockDay, remoteSigOfLocal = remoteOverride.localSigOfRemoteLCSS)
            .withLocalSigOfRemote(LNParams.keys.extendedNodeKey.privateKey)

        val nextLocalSU = restoredLCSS.stateUpdate(isTerminal = true)
        if (localBalance < 0) throw new LightningException("Provided updated local balance is larger than capacity")
        if (remoteOverride.localUpdates < hc.lastCrossSignedState.remoteUpdates) throw new LightningException("Provided local update number from remote host is wrong")
        if (remoteOverride.remoteUpdates < hc.lastCrossSignedState.localUpdates) throw new LightningException("Provided remote update number from remote host is wrong")
        if (remoteOverride.blockDay < hc.lastCrossSignedState.blockDay) throw new LightningException("Provided override blockday from remote host is not acceptable")
        require(restoredLCSS.verifyRemoteSig(hc.announce.nodeId), "Provided override signature from remote host is wrong")
        STORESENDBECOME(restoreCommits(restoredLCSS, hc.announce), OPEN, nextLocalSU)
        // They may send a new StateUpdate right after overriding
        waitingUpdate = true


      case (_, routingData: RoutingData, _) => throw CMDAddImpossible(routingData, ERR_CAN_NOT_ADD)
      case (null, waitReply: WaitRemoteHostedReply, null) => super.become(waitReply, WAIT_FOR_INIT)
      case (null, hc: HostedCommits, null) if hc.getError.isDefined => super.become(hc, SUSPENDED)
      case (null, hc: HostedCommits, null) => super.become(hc, SLEEPING)
      case _ =>
    }

    // Change has been processed without failures
    events onProcessSuccess Tuple3(me, data, change)
  }

  def syncAndResend(hc: HostedCommits, leftovers: Vector[LNDirectionalMessage], lcss: LastCrossSignedState, spec: CommitmentSpec) = {
    // Forget about remote, re-send our LCSS and all non-cross-signed local updates, finally sign if local leftovers are indeed present
    val hc1 = hc.copy(futureUpdates = leftovers.filter { case _ \ isLocal => isLocal }, lastCrossSignedState = lcss, localSpec = spec)
    SEND(hc1.lastCrossSignedState +: hc1.nextLocalUpdates:_*)
    // CMDProceed is on ChannelManager
    BECOME(hc1, OPEN)
  }

  def restoreCommits(localLCSS: LastCrossSignedState, ann: NodeAnnouncement) = {
    val inHtlcs = for (updateAddHtlc <- localLCSS.incomingHtlcs) yield Htlc(incoming = true, updateAddHtlc)
    val outHtlcs = for (updateAddHtlc <- localLCSS.outgoingHtlcs) yield Htlc(incoming = false, updateAddHtlc)
    val localSpec = CommitmentSpec(feeratePerKw = 0L, localLCSS.localBalanceMsat, localLCSS.remoteBalanceMsat, htlcs = (inHtlcs ++ outHtlcs).toSet)
    HostedCommits(ann, localLCSS, futureUpdates = Vector.empty, localSpec, updateOpt = None, localError = None, remoteError = None, System.currentTimeMillis)
  }

  // CMDProceed is on ChannelManager
  def localSuspend(hc: HostedCommits, errCode: ByteVector) = {
    val localError = Error(channelId = hc.channelId, data = errCode)
    val hc1 = hc.modify(_.localError) setTo Some(localError)
    STORESENDBECOME(hc1, SUSPENDED, localError)
  }
}

trait ChannelListener {
  type Malfunction = (Channel, Throwable)
  type Incoming = (Channel, ChannelData, Any)
  type Transition = (Channel, ChannelData, String, String)
  def onProcessSuccess: PartialFunction[Incoming, Unit] = none
  def onException: PartialFunction[Malfunction, Unit] = none
  def onBecome: PartialFunction[Transition, Unit] = none

  def fulfillReceived(fulfill: UpdateFulfillHtlc): Unit = none
  def incomingCrossSigned: Unit = none
}
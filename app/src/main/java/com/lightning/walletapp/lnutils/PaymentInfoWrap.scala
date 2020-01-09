package com.lightning.walletapp.lnutils

import spray.json._
import com.lightning.walletapp.ln._
import com.lightning.walletapp.ln.wire._
import com.lightning.walletapp.ln.Channel._
import com.lightning.walletapp.ln.LNParams._
import com.lightning.walletapp.ln.PaymentInfo._
import com.lightning.walletapp.lnutils.JsonHttpUtils._
import com.lightning.walletapp.lnutils.ImplicitConversions._
import com.lightning.walletapp.lnutils.ImplicitJsonFormats._

import com.lightning.walletapp.helper.{AES, RichCursor}
import fr.acinq.bitcoin.{Crypto, MilliSatoshi, Transaction}
import com.lightning.walletapp.lnutils.olympus.{CerberusAct, OlympusWrap}
import com.lightning.walletapp.ln.wire.LightningMessageCodecs.cerberusPayloadCodec
import com.lightning.walletapp.ln.wire.LightningMessageCodecs
import com.lightning.walletapp.ln.RoutingInfoTag.PaymentRoute
import com.lightning.walletapp.ln.crypto.Sphinx.PublicKeyVec
import com.lightning.walletapp.ChannelManager
import fr.acinq.bitcoin.Crypto.PublicKey
import com.lightning.walletapp.Utils.app
import scodec.bits.ByteVector


object PaymentInfoWrap extends PaymentInfoBag with ChannelListener { me =>
  var acceptedPayments = Map.empty[ByteVector, RoutingData]
  var unsentPayments = Map.empty[ByteVector, RoutingData]
  var newRoutesOrGiveUp: RoutingData => Unit = _
  var failOnUI: RoutingData => Unit = _

  def extractPreimage(candidateTx: Transaction) = {
    val fulfills = candidateTx.txIn.map(_.witness.stack) collect {
      case Seq(_, pre, _) if pre.size == 32 => UpdateFulfillHtlc(ByteVector.empty, 0L, pre)
      case Seq(_, _, _, pre, _) if pre.size == 32 => UpdateFulfillHtlc(ByteVector.empty, 0L, pre)
    }

    fulfills foreach updOkOutgoing
    if (fulfills.nonEmpty) uiNotify
  }

  protected def doUpdOkIncoming(upd: UpdateAddHtlc) = db.change(PaymentTable.updOkIncomingSql, upd.amountMsat, System.currentTimeMillis, upd.paymentHash) // TODO: WRONG, need a compound amount here
  protected def doUpdOkOutgoing(upd: UpdateFulfillHtlc) = db.change(PaymentTable.updOkOutgoingSql, upd.paymentPreimage, upd.paymentHash)
  protected def doUpdStatus(status: Int, hash: ByteVector) = db.change(PaymentTable.updStatusSql, status, hash)

  protected def doGetPaymentInfo(hash: ByteVector) = {
    val paymentCursor = db.select(PaymentTable.selectSql, hash)
    RichCursor(paymentCursor).headTry(toPaymentInfo).toOption
  }

  protected def doUpdatePendingOutgoing(rd: RoutingData) = db txWrap {
    // firstMsat also gets updated because we may attempt a second try with different amount
    db.change(PaymentTable.updLastParamsOutgoingSql, rd.firstMsat, rd.lastMsat, rd.pr.paymentHash)
    db.change(PaymentTable.newSql, rd.pr.toJson, NOIMAGE, 0 /* outgoing */, WAITING, System.currentTimeMillis,
      PaymentDescription(rd.action, rd.pr.description).toJson, rd.pr.paymentHash, rd.firstMsat, rd.lastMsat)
  }

  def addPendingPayment(rd: RoutingData) = {
    // Add payment to unsentPayments and try to resolve it later
    unsentPayments = unsentPayments.updated(rd.pr.paymentHash, rd)
    updatePendingOutgoing(rd)
    resolvePending
    uiNotify
  }

  def resolvePending = {
    // When accepted by channel: gets removed in outPaymentAccepted
    // When uncapable chan becomes online: persists, waits for capable channel
    // When no routes found or any other error happens: gets removed from map in failOnUI
    if (ChannelManager.currentBlocksLeft.isDefined) unsentPayments.values foreach fetchAndSend
  }

  def uiNotify = app.getContentResolver.notifyChange(db sqlPath PaymentTable.table, null)
  def fetchAndSend(rd: RoutingData) = ChannelManager.fetchRoutes(rd).foreach(ChannelManager.sendEither(_, failOnUI), _ => me failOnUI rd)
  def byQuery(mayHaveInjectionQuery: String) = db.select(PaymentTable.searchSql, s"${mayHaveInjectionQuery.noInjection}*")
  def byRecent = db select PaymentTable.selectRecentSql

  def toPaymentInfo(rc: RichCursor) = PaymentInfo(rawPr = rc string PaymentTable.pr, hash = rc string PaymentTable.hash,
    preimage = rc string PaymentTable.preimage, incoming = rc int PaymentTable.incoming, status = rc int PaymentTable.status,
    stamp = rc long PaymentTable.stamp, description = rc string PaymentTable.description, firstMsat = rc long PaymentTable.firstMsat,
    lastMsat = rc long PaymentTable.lastMsat)

  def recordRoutingDataWithPr(extraRoutes: Vector[PaymentRoute], amount: MilliSatoshi, preimage: ByteVector, description: String): RoutingData = {
    val pr = PaymentRequest(chainHash, Some(amount), Crypto sha256 preimage, keys.extendedNodeKey.privateKey, description, fallbackAddress = None, extraRoutes)
    val rd = app.emptyRD(pr, amount.toLong)

    db.change(PaymentTable.newVirtualSql, rd.queryText, pr.paymentHash)
    db.change(PaymentTable.newSql, pr.toJson, preimage, 1 /* incoming */, WAITING,
      System.currentTimeMillis, PaymentDescription(None, description).toJson, pr.paymentHash, amount.toLong,
      0L) // -> lastMsat with routing fees, may later be updated if this payment becomes a reflexive one

    uiNotify
    rd
  }

  override def outPaymentAccepted(rd: RoutingData) = {
    acceptedPayments = acceptedPayments.updated(rd.pr.paymentHash, rd)
    unsentPayments = unsentPayments - rd.pr.paymentHash
    updatePendingOutgoing(rd)
  }

  override def fulfillReceived(ok: UpdateFulfillHtlc) = db txWrap {
    // Do not wait for their next CommitSig and save a preimage right away, also make this payment searchable
    for (rd <- acceptedPayments get ok.paymentHash) db.change(PaymentTable.newVirtualSql, rd.queryText, rd.pr.paymentHash)
    updOkOutgoing(ok)
  }

  override def onSettled(cs: Commitments) = {
    // Mark failed and fulfilled, upload backups

    db txWrap {
      for (updateAddHtlc <- cs.localSpec.fulfilledIncoming) updOkIncoming(updateAddHtlc)
      for (Htlc(false, add) <- cs.localSpec.malformed) updStatus(FAILURE, add.paymentHash)
      for (Htlc(false, add) \ failReason <- cs.localSpec.failed) {

        val rdOpt = acceptedPayments get add.paymentHash
        rdOpt map parseFailureCutRoutes(failReason) match {
          // Try to use reamining routes or fetch new ones if empty
          // but account for possibility of rd not being in place

          case Some(Some(rd1) \ excludes) =>
            for (badEntity <- excludes) BadEntityWrap.putEntity.tupled(badEntity)
            ChannelManager.sendEither(useFirstRoute(rd1.routes, rd1), newRoutesOrGiveUp)

          case _ =>
            // May happen after app has been restarted
            // also when someone sends an unparsable error
            updStatus(FAILURE, add.paymentHash)
        }
      }
    }

    uiNotify
    if (cs.localSpec.fulfilledIncoming.nonEmpty) {
      val vulnerableStates = ChannelManager.all.flatMap(getVulnerableRevVec).toMap
      getCerberusActs(vulnerableStates).foreach(olympusWrap.tellClouds)
    }

    if (cs.localSpec.fulfilled.nonEmpty) {
      // This could be a memo-resolving payment
      olympusWrap tellClouds OlympusWrap.CMDStart
    }
  }

  def getVulnerableRevVec(chan: Channel) = chan.getCommits match {
    case Some(normalCommits: NormalCommits) if isOperational(chan) =>
      // Find previous states where amount is lower by more than 10000 SAT
      val amountThreshold = normalCommits.remoteCommit.spec.toRemoteMsat - 10000000L
      val cursor = db.select(RevokedInfoTable.selectLocalSql, normalCommits.channelId, amountThreshold)
      def toTxidAndInfo(rc: RichCursor) = Tuple2(rc string RevokedInfoTable.txId, rc string RevokedInfoTable.info)
      RichCursor(cursor) vec toTxidAndInfo

    case _ =>
      // Hosted channels
      // Closing channges
      Vector.empty
  }

  def getCerberusActs(infos: Map[String, String] = Map.empty) = {
    // Remove currently pending infos and limit max number of uploads
    val notPendingInfos = infos -- olympusWrap.pendingWatchTxIds take 100

    val encrypted = for {
      txid \ revInfo <- notPendingInfos
      txidBytes = ByteVector.fromValidHex(txid).toArray
      revInfoBytes = ByteVector.fromValidHex(revInfo).toArray
      enc = AES.encBytes(revInfoBytes, txidBytes)
    } yield txid -> enc

    for {
      pack <- encrypted grouped 20
      txids \ zygotePayloads = pack.unzip
      halfTxIds = for (txid <- txids) yield txid take 16
      cp = CerberusPayload(zygotePayloads.toVector, halfTxIds.toVector)
      bin = LightningMessageCodecs.serialize(cerberusPayloadCodec encode cp)
    } yield CerberusAct(bin, Nil, "cerberus/watch", txids.toVector)
  }

  override def onProcessSuccess = {
    case (_: NormalChannel, wbr: WaitBroadcastRemoteData, CMDChainTipKnown) if wbr.isLost =>
      // We don't allow manual deletion here as funding may just not be locally visible yet
      app.kit.wallet.removeWatchedScripts(app.kit fundingPubScript wbr)
      db.change(ChannelTable.killSql, wbr.commitments.channelId)

    case (chan: NormalChannel, norm @ NormalData(_, _, Some(spendTx), _, _), CMDChainTipKnown) =>
      // Must be careful here: unknown spend might be a future commit so only publish local commit if that spend if deeply buried
      // this way a published local commit can not possibly trigger a punishment and will be failed right away with channel becoming CLOSED
      ChannelManager getStatus spendTx.txid match { case depth \ false if depth > blocksPerDay => chan startLocalClose norm case _ => }

    case (_: NormalChannel, close: ClosingData, CMDChainTipKnown) if close.canBeRemoved =>
      // Either a lot of time has passed or ALL closing transactions have enough confirmations
      app.kit.wallet.removeWatchedScripts(app.kit closingPubKeyScripts close)
      app.kit.wallet.removeWatchedScripts(app.kit fundingPubScript close)
      db.change(RevokedInfoTable.killSql, close.commitments.channelId)
      db.change(ChannelTable.killSql, close.commitments.channelId)
  }

  override def onBecome = {
    case (_, _, SLEEPING, OPEN) =>
      // We may have payments waiting
      resolvePending

    case (_, _, WAIT_FUNDING_DONE, OPEN) =>
      // We may have a channel upload act waiting
      olympusWrap tellClouds OlympusWrap.CMDStart
  }
}

object ChannelWrap {
  def doPut(chanId: ByteVector, data: String) = db txWrap {
    // Insert and then update because of INSERT IGNORE effects
    db.change(ChannelTable.newSql, chanId, data)
    db.change(ChannelTable.updSql, data, chanId)
  }

  def put(data: ChannelData) = data match {
    case refund: RefundingData if refund.remoteLatestPoint.isEmpty => // Skipping empty refund
    case hasNorm: HasNormalCommits => doPut(hasNorm.commitments.channelId, '1' + hasNorm.toJson.toString)
    case hostedCommits: HostedCommits => doPut(hostedCommits.channelId, '2' + hostedCommits.toJson.toString)
    case otherwise => throw new RuntimeException(s"Can not presist this channel data type: $otherwise")
  }

  def doGet(database: LNOpenHelper) =
    RichCursor(database select ChannelTable.selectAllSql).vec(_ string ChannelTable.data) map {
      case rawChanData if '1' == rawChanData.head => to[HasNormalCommits](rawChanData substring 1)
      case rawChanData if '2' == rawChanData.head => to[HostedCommits](rawChanData substring 1)
      case otherwise => throw new RuntimeException(s"Can not deserialize: $otherwise")
    }
}

object BadEntityWrap {
  val putEntity = (entity: String, span: Long, msat: Long) => {
    db.change(BadEntityTable.newSql, entity, System.currentTimeMillis + span, msat)
    db.change(BadEntityTable.updSql, System.currentTimeMillis + span, msat, entity)
  }

  def findRoutes(from: PublicKeyVec, targetId: PublicKey, rd: RoutingData) = {
    val cursor = db.select(BadEntityTable.selectSql, System.currentTimeMillis, rd.firstMsat)
    // Both shortChannelId and nodeId are recorded in a same table for performance reasons, discerned by length
    val badNodes \ badChans = RichCursor(cursor).set(_ string BadEntityTable.resId).partition(_.length > 60)

    val targetStr = targetId.toString
    val fromAsStr = from.map(_.toString).toSet
    val finalBadNodes = badNodes - targetStr -- fromAsStr // Remove source and sink nodes because they could be blacklisted earlier
    olympusWrap findRoutes OutRequest(rd.firstMsat / 1000L, finalBadNodes, badChans.map(_.toLong), fromAsStr, targetStr)
  }
}
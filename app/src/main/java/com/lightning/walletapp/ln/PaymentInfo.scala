package com.lightning.walletapp.ln

import scala.collection.JavaConverters._
import com.lightning.walletapp.ln.wire._
import com.lightning.walletapp.ln.crypto.Sphinx._
import com.lightning.walletapp.ln.RoutingInfoTag._
import com.lightning.walletapp.lnutils.ImplicitJsonFormats._

import fr.acinq.bitcoin.{MilliSatoshi, Transaction}
import com.lightning.walletapp.lnutils.JsonHttpUtils.to
import com.lightning.walletapp.ln.Tools.runAnd
import com.lightning.walletapp.ChannelManager
import java.util.concurrent.ConcurrentHashMap
import fr.acinq.bitcoin.Crypto.PublicKey
import scodec.bits.ByteVector


object PaymentInfo {
  final val WAITING = 1
  final val SUCCESS = 2
  final val FAILURE = 3

  type FullOrEmptyRD = Either[RoutingData, RoutingData]
  final val NOIMAGE = ByteVector.fromValidHex("3030303030303030")
  private[this] var replacedChans = Set.empty[Long]

  def buildOnion(keys: PublicKeyVec, payloads: Vector[PerHopPayload], assoc: ByteVector): PacketAndSecrets = {
    require(keys.size == payloads.size, "Count mismatch: there should be exactly as much payloads as node pubkeys")
    PaymentPacket.create(Tools.randomPrivKey, keys, for (payload <- payloads) yield payload.encode, assoc)
  }

  def useFirstRoute(rest: PaymentRouteVec, rd: RoutingData) =
    if (rest.isEmpty) Left(rd) else useRoute(rest.head, rest.tail, rd)

  def useRoute(route: PaymentRoute, rest: PaymentRouteVec, rd: RoutingData): FullOrEmptyRD = {
    val firstPayeeCltvAdjustedExpiry = ChannelManager.currentHeight + rd.pr.adjustedMinFinalCltvExpiry
    val payloadVec = FinalLegacyPayload(rd.firstMsat, firstPayeeCltvAdjustedExpiry) +: Vector.empty[PerHopPayload]
    val finalHopState = Tuple4(payloadVec, Vector.empty[PublicKey], rd.firstMsat, firstPayeeCltvAdjustedExpiry)

    // Walk in reverse direction from receiver to sender and accumulate cltv deltas + fees
    val (allPayloads, nodeIds, lastMsat, lastExpiry) = route.reverse.foldLeft(finalHopState) { case (payloads, nodes, msat, expiry) \ hop =>
      (RelayLegacyPayload(hop.shortChannelId, msat, expiry) +: payloads, hop.nodeId +: nodes, hop.fee(msat) + msat, hop.cltvExpiryDelta + expiry)
    }

    val isCltvBreach = lastExpiry - ChannelManager.currentHeight > LNParams.maxCltvDelta
    if (LNParams.isFeeBreach(route, rd.firstMsat, percent = 100L) || isCltvBreach) useFirstRoute(rest, rd) else {
      val onionPacket = buildOnion(keys = nodeIds :+ rd.pr.nodeId, payloads = allPayloads, assoc = rd.pr.paymentHash)
      val rd2 = rd.copy(routes = rest, usedRoute = route, onion = onionPacket, lastMsat = lastMsat, lastExpiry = lastExpiry)
      Right(rd2)
    }
  }

  def without(routes: PaymentRouteVec, fun: Hop => Boolean) = routes.filterNot(_ exists fun)
  def failFinalPayloadSpec(fail: FailureMessage, finalPayloadSpec: FinalPayloadSpec) = failHtlc(finalPayloadSpec.packet, fail, finalPayloadSpec.add)
  def failHtlc(packet: DecryptedPacket, fail: FailureMessage, add: UpdateAddHtlc) = CMDFailHtlc(FailurePacket.create(packet.sharedSecret, fail), add)

  def withoutChan(shortId: Long, rd: RoutingData, span: Long, msat: Long) = {
    val routesWithoutBadChannels = without(rd.routes, _.shortChannelId == shortId)
    val blackListedChan = Tuple3(shortId.toString, span, msat)
    val rd1 = rd.copy(routes = routesWithoutBadChannels)
    Some(rd1) -> Vector(blackListedChan)
  }

  def withoutNodes(badNodes: PublicKeyVec, rd: RoutingData, span: Long) = {
    val routesWithoutBadNodes = without(rd.routes, badNodes contains _.nodeId)
    val blackListedNodes = for (node <- badNodes) yield (node.toString, span, 0L)
    val rd1 = rd.copy(routes = routesWithoutBadNodes)
    Some(rd1) -> blackListedNodes
  }

  def replaceChan(nodeKey: PublicKey, rd: RoutingData, upd: ChannelUpdate) = {
    // In some cases we can just replace a faulty hop with a supplied one
    // but only do this once per each channel to avoid infinite loops

    val rd1 = rd.copy(routes = rd.usedRoute.map {
      case keepMe if keepMe.nodeId != nodeKey => keepMe
      case _ => upd.toHop(nodeKey)
    } +: rd.routes)

    // Prevent endless loop by marking this channel
    replacedChans += upd.shortChannelId
    Some(rd1) -> Vector.empty
  }

  def parseFailureCutRoutes(fail: UpdateFailHtlc)(rd: RoutingData) =
    FailurePacket.decrypt(packet = fail.reason, rd.onion.sharedSecrets) map {
      case DecryptedFailurePacket(nodeKey, ExpiryTooFar | PaymentTimeout) if nodeKey == rd.pr.nodeId => None -> Vector.empty
      case DecryptedFailurePacket(nodeKey, _: IncorrectOrUnknownPaymentDetails) if nodeKey == rd.pr.nodeId => None -> Vector.empty
      case DecryptedFailurePacket(nodeKey, u: ExpiryTooSoon) if !replacedChans.contains(u.update.shortChannelId) => replaceChan(nodeKey, rd, u.update)
      case DecryptedFailurePacket(nodeKey, u: FeeInsufficient) if !replacedChans.contains(u.update.shortChannelId) => replaceChan(nodeKey, rd, u.update)
      case DecryptedFailurePacket(nodeKey, u: IncorrectCltvExpiry) if !replacedChans.contains(u.update.shortChannelId) => replaceChan(nodeKey, rd, u.update)

      case DecryptedFailurePacket(nodeKey, u: Update) =>
        val isHonest = Announcements.checkSig(u.update, nodeKey)
        if (!isHonest) withoutNodes(Vector(nodeKey), rd, 86400 * 7 * 1000)
        else rd.usedRoute.collectFirst { case payHop if payHop.nodeId == nodeKey =>
          withoutChan(u.update.shortChannelId, rd, 180 * 1000, rd.firstMsat)
        } getOrElse withoutNodes(Vector(nodeKey), rd, 180 * 1000)

      case DecryptedFailurePacket(nodeKey, PermanentNodeFailure) => withoutNodes(Vector(nodeKey), rd, 86400 * 7 * 1000)
      case DecryptedFailurePacket(nodeKey, RequiredNodeFeatureMissing) => withoutNodes(Vector(nodeKey), rd, 86400 * 1000)
      case DecryptedFailurePacket(nodeKey, _: BadOnion) => withoutNodes(Vector(nodeKey), rd, 180 * 1000)

      case DecryptedFailurePacket(nodeKey, UnknownNextPeer | PermanentChannelFailure | RequiredChannelFeatureMissing) =>
        rd.usedRoute.collectFirst { case payHop if payHop.nodeId == nodeKey =>
          withoutChan(payHop.shortChannelId, rd, 86400 * 7 * 1000, 0L)
        } getOrElse withoutNodes(Vector(nodeKey), rd, 180 * 1000)

      case DecryptedFailurePacket(nodeKey, _) =>
        rd.usedRoute.collectFirst { case payHop if payHop.nodeId == nodeKey =>
          withoutChan(payHop.shortChannelId, rd, 180 * 1000, rd.firstMsat)
        } getOrElse withoutNodes(Vector(nodeKey), rd, 180 * 1000)

    } getOrElse {
      val cut = rd.usedRoute drop 1 dropRight 1
      withoutNodes(cut.map(_.nodeId), rd, 60 * 1000)
    }
}

case class RoutingData(pr: PaymentRequest, routes: PaymentRouteVec, usedRoute: PaymentRoute, onion: PacketAndSecrets,
                       firstMsat: Long /* amount without off-chain fee */, lastMsat: Long /* with off-chain fee added */,
                       lastExpiry: Long, callsLeft: Int, action: Option[PaymentAction], fromHostedOnly: Boolean) {

  // Empty used route means we're sending to peer and its nodeId should be our targetId
  def nextNodeId(route: PaymentRoute) = route.headOption.map(_.nodeId) getOrElse pr.nodeId
  lazy val queryText = s"${pr.description} ${pr.nodeId.toString} ${pr.paymentHash.toHex}"
  lazy val isReflexive = pr.nodeId == LNParams.keys.extendedNodeKey.publicKey
}

case class PaymentInfo(rawPr: String, hash: String, preimage: String, incoming: Int, status: Int,
                       stamp: Long, description: String, firstMsat: Long, lastMsat: Long) {

  // Incoming lastMsat becomes > 0 once reflexive
  val isLooper = incoming == 1 && lastMsat != 0
  // What payee gets, may change for outgoing
  val firstSum = MilliSatoshi(firstMsat)

  // Decode on demand for performance
  lazy val pr = to[PaymentRequest](rawPr)
  lazy val paymentHash = ByteVector.fromValidHex(hash)
  lazy val paymentPreimage = ByteVector.fromValidHex(preimage)

  // Back compat: use default object if source is not json
  lazy val pd = try to[PaymentDescription](description) catch {
    case _: Throwable => PaymentDescription(None, description)
  }
}

trait PaymentInfoBag {
  type PaymentInfoOpt = Option[PaymentInfo]
  private val cache = new ConcurrentHashMap[ByteVector, PaymentInfoOpt].asScala

  def getPaymentInfo(hash: ByteVector): PaymentInfoOpt = cache.getOrElseUpdate(op = doGetPaymentInfo(hash), key = hash)
  def updStatus(status: Int, hash: ByteVector): Unit = runAnd(cache remove hash) { doUpdStatus(status, hash) /* clear cache first */ }
  def updatePendingOutgoing(rd: RoutingData): Unit = runAnd(cache remove rd.pr.paymentHash) { doUpdatePendingOutgoing(rd) /* clear cache first */ }
  def updOkOutgoing(upd: UpdateFulfillHtlc): Unit = runAnd(cache remove upd.paymentHash) { doUpdOkOutgoing(upd) /* clear cache first */ }
  def updOkIncoming(upd: UpdateAddHtlc): Unit = runAnd(cache remove upd.paymentHash) { doUpdOkIncoming(upd) /* clear cache first */ }

  protected def doGetPaymentInfo(hash: ByteVector): PaymentInfoOpt
  protected def doUpdStatus(status: Int, hash: ByteVector): Unit
  protected def doUpdatePendingOutgoing(rd: RoutingData): Unit
  protected def doUpdOkOutgoing(upd: UpdateFulfillHtlc): Unit
  protected def doUpdOkIncoming(upd: UpdateAddHtlc): Unit
  def extractPreimage(tx: Transaction)
}

// Payment actions

sealed trait PaymentAction {
  val domain: Option[String]
  val finalMessage: String
}

case class MessageAction(domain: Option[String], message: String) extends PaymentAction {
  val finalMessage = s"<br>${message take 1440}"
}

case class UrlAction(domain: Option[String], description: String, url: String) extends PaymentAction {
  val finalMessage = s"<br>${description take 1440}<br><br><font color=#0000FF><tt>$url</tt></font><br>"
  require(domain.forall(url.contains), s"Action domain=$domain mismatches callback=$url")
  require(url startsWith "https://", "Action url is not HTTPS")
}

case class AESAction(domain: Option[String], description: String, ciphertext: String, iv: String) extends PaymentAction {
  val ciphertextBytes = ByteVector.fromValidBase64(ciphertext).take(1024 * 4).toArray // up to ~2kb of encrypted data
  val ivBytes = ByteVector.fromValidBase64(iv).take(24).toArray // 16 bytes
  val finalMessage = s"<br>${description take 1440}"
}

// Previously `PaymentInfo.description` was just raw text
// once actions were added it became json which encodes this class
case class PaymentDescription(action: Option[PaymentAction], text: String)
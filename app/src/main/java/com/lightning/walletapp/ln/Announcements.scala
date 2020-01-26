package com.lightning.walletapp.ln

import com.lightning.walletapp.ln.wire._
import com.lightning.walletapp.ln.wire.LightningMessageCodecs._
import fr.acinq.bitcoin.Crypto.{PrivateKey, PublicKey, verifySignature}
import fr.acinq.bitcoin.{Crypto, LexicographicalOrdering}
import scodec.bits.{ByteVector, BitVector}
import shapeless.HNil


object Announcements { me =>
  private def hashTwice(attempt: BitVectorAttempt) = Crypto hash256 serialize(attempt)
  private def nodeAnnouncementWitnessEncode(timestamp: Long, nodeId: PublicKey, rgbColor: RGB, alias: String, features: ByteVector, addresses: NodeAddressList, unknownFields: ByteVector) =
    me hashTwice LightningMessageCodecs.nodeAnnouncementWitness.encode(features :: timestamp :: nodeId :: rgbColor :: alias :: addresses :: unknownFields :: HNil)

  private def channelUpdateWitnessEncode(chainHash: ByteVector, shortChannelId: Long, timestamp: Long, messageFlags: Byte, channelFlags: Byte, cltvExpiryDelta: Int, htlcMinimumMsat: Long, feeBaseMsat: Long, feeProportionalMillionths: Long, htlcMaximumMsat: Option[Long], unknownFields: ByteVector) =
    me hashTwice LightningMessageCodecs.channelUpdateWitness.encode(chainHash :: shortChannelId :: timestamp :: messageFlags :: channelFlags :: cltvExpiryDelta :: htlcMinimumMsat :: feeBaseMsat :: feeProportionalMillionths :: htlcMaximumMsat :: unknownFields :: HNil)

  // The creating node MUST set node-id-1 and node-id-2 to the public keys of the
  // two nodes who are operating the channel, such that node-id-1 is the numerically-lesser
  // of the two DER encoded keys sorted in ascending numerical order

  def isNode1(channelFlags: Byte) = (channelFlags & 1) == 0
  def isEnabled(channelFlags: Byte) = (channelFlags & 2) == 0
  def isNode1(localNodeId: PublicKey, remoteNodeId: PublicKey) = LexicographicalOrdering.isLessThan(localNodeId, remoteNodeId)
  def makeMessageFlags(hasOptionChannelHtlcMax: Boolean) = BitVector.bits(hasOptionChannelHtlcMax :: Nil).padLeft(8).toByte(true)
  def makeChannelFlags(isNode1: Boolean, enable: Boolean) = BitVector.bits(!enable :: !isNode1 :: Nil).padLeft(8).toByte(true)

  def checkSig(ann: NodeAnnouncement): Boolean =
    verifySignature(nodeAnnouncementWitnessEncode(ann.timestamp, ann.nodeId, ann.rgbColor, ann.alias,
      ann.features, ann.addresses, unknownFields = ByteVector.empty), wire2der(ann.signature), ann.nodeId)

  def checkSig(upd: ChannelUpdate, nodeId: PublicKey): Boolean =
    verifySignature(channelUpdateWitnessEncode(upd.chainHash, upd.shortChannelId, upd.timestamp, upd.messageFlags,
      upd.channelFlags, upd.cltvExpiryDelta, upd.htlcMinimumMsat, upd.feeBaseMsat, upd.feeProportionalMillionths,
      upd.htlcMaximumMsat, unknownFields = ByteVector.empty), wire2der(upd.signature), nodeId)
}
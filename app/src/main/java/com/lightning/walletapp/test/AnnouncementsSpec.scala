package com.lightning.walletapp.test

import com.lightning.walletapp.ln.Announcements._
import fr.acinq.bitcoin.Crypto.PublicKey
import scodec.bits.ByteVector

class AnnouncementsSpec {
  def allTests = {
    {
      println("check nodeId1/nodeId2 lexical ordering")
      val node1 = PublicKey(ByteVector fromValidHex "027710df7a1d7ad02e3572841a829d141d9f56b17de9ea124d2f83ea687b2e0461")
      val node2 = PublicKey(ByteVector fromValidHex "0306a730778d55deec162a74409e006034a24c46d541c67c6c45f89a2adde3d9b4")
      // NB: node1 < node2
      assert(isNode1(node1, node2))
      assert(!isNode1(node2, node1))
    }
  }
}
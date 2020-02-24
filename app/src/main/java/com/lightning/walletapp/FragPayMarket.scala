package com.lightning.walletapp

import android.os.Bundle
import android.support.v4.app.Fragment
import com.lightning.walletapp.Utils.app
import android.view.{LayoutInflater, View, ViewGroup}


class FragPayMarket extends Fragment {
  override def onCreateView(inf: LayoutInflater, viewGroup: ViewGroup, bundle: Bundle) =  inf.inflate(R.layout.frag_view_pager_paymarket, viewGroup, false)
  override def onViewCreated(view: View, state: Bundle) = if (app.isAlive) println(s"-- View created")
  override def onDestroy = super.onDestroy
  override def onResume = super.onResume
}

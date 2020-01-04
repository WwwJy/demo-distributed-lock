package com.sifei.info.locks.sdk.core;

public interface ZKNotify {

    String getAddress();

    void successConnNotify();

    void nodeDeleteNotify();
}

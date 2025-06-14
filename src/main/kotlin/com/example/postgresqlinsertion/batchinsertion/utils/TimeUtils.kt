package com.example.postgresqlinsertion.batchinsertion.utils

fun getTimeString(time: Long):String {
    val min = (time / 1000) / 60
    val sec = (time / 1000) % 60
    val ms = time - min*1000*60 - sec*1000
    return "$min min, $sec sec $ms ms"
}
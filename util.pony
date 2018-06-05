primitive Util
  fun copy_cpointer(cs: Pointer[U8] iso, len: USize = 0): String =>
    recover String.copy_cpointer(consume cs, len) end

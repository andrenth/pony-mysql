primitive Util
  fun from_cstring(cs: Pointer[U8] iso, len: USize = 0): String =>
    recover String.from_cstring(consume cs, len) end

primitive Util
  fun copy_cstring(cs: Pointer[U8] val, len: USize = 0): String =>
    recover String.copy_cstring(cs, len) end

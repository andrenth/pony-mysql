primitive ConnectTimeout            fun apply(): USize => 0
primitive Compress                  fun apply(): USize => 1
primitive NamedPipe                 fun apply(): USize => 2
primitive InitCommand               fun apply(): USize => 3
primitive ReadDefaultFile           fun apply(): USize => 4
primitive ReadDefaultGroup          fun apply(): USize => 5
primitive CharsetDir                fun apply(): USize => 6
primitive CharsetName               fun apply(): USize => 7
primitive LocalInFile               fun apply(): USize => 8
primitive Protocol                  fun apply(): USize => 9
primitive SharedMemoryBaseName      fun apply(): USize => 10
primitive ReadTimeout               fun apply(): USize => 11
primitive WriteTimeout              fun apply(): USize => 12
primitive UseResult                 fun apply(): USize => 13
primitive UseRemoteConnection       fun apply(): USize => 14
primitive UseEmbeddedConnection     fun apply(): USize => 15
primitive GuessConnection           fun apply(): USize => 16
primitive ClientIP                  fun apply(): USize => 17
primitive SecureAuth                fun apply(): USize => 18
primitive ReportDataTruncation      fun apply(): USize => 19
primitive Reconnect                 fun apply(): USize => 20
primitive SSLVerifyServerCert       fun apply(): USize => 21
primitive PluginDir                 fun apply(): USize => 22
primitive DefaultAuth               fun apply(): USize => 23
primitive Bind                      fun apply(): USize => 24
primitive SSLKey                    fun apply(): USize => 25
primitive SSLCert                   fun apply(): USize => 26
primitive SSLCA                     fun apply(): USize => 27
primitive SSLCAPath                 fun apply(): USize => 28
primitive SSLCipher                 fun apply(): USize => 29
primitive SSLCRL                    fun apply(): USize => 30
primitive SSLCRLPath                fun apply(): USize => 31
primitive ConnectAttrReset          fun apply(): USize => 32
primitive ConnectAttrAdd            fun apply(): USize => 33
primitive ConnectAttrDelete         fun apply(): USize => 34
primitive ServerPublicKey           fun apply(): USize => 35
primitive EnableCleartextPlugin     fun apply(): USize => 36
primitive CanHandleExpiredPasswords fun apply(): USize => 37

type ClientOption is
  ( ConnectTimeout
  | Compress
  | NamedPipe
  | InitCommand
  | ReadDefaultFile
  | ReadDefaultGroup
  | CharsetDir
  | CharsetName
  | LocalInFile
  | Protocol
  | SharedMemoryBaseName
  | ReadTimeout
  | WriteTimeout
  | UseResult
  | UseRemoteConnection
  | UseEmbeddedConnection
  | GuessConnection
  | ClientIP
  | SecureAuth
  | ReportDataTruncation
  | Reconnect
  | SSLVerifyServerCert
  | PluginDir
  | DefaultAuth
  | Bind
  | SSLKey
  | SSLCert
  | SSLCA
  | SSLCAPath
  | SSLCipher
  | SSLCRL
  | SSLCRLPath
  | ConnectAttrReset
  | ConnectAttrAdd
  | ConnectAttrDelete
  | ServerPublicKey
  | EnableCleartextPlugin
  | CanHandleExpiredPasswords
  )

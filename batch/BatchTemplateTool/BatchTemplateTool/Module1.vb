Imports System.Text
Imports Common
Imports Common.Constants
Imports Common.Log.Manager
Imports System.Configuration

Module ExitCodes
    Public Const ExitCodeError As Integer = -1           ' 実行中エラー
    Public Const ExitCodeSuccess As Integer = 0          ' 正常終了
    Public Const ExitCodeInvalidArgument As Integer = 1  ' チェックエラー
    Public Const ExitCodeSkip As Integer = 9             ' 処理スキップ（対象なし）
End Module

Module Module1

    Private sw As Stopwatch = Nothing

    Public Function Main(args As String()) As Integer
        Dim result As Integer = ExitCodeSuccess

        Try
            '初期設定
            SetSystemCommonConfig()
            'イベント開始記録
            StratLog()


            'イベント結果記録
            EndLog()
        Catch ex As Exception
            EndLog(ex.ToString)
        End Try

        Return result
    End Function

    Private Sub SetSystemCommonConfig()

#If DEBUG Then
        ConfigManager.RunEnvironment = CommonConstants.EnvironmentType.Staging
#Else
        ConfigManager.RunEnvironment = CommonConstants.EnvironmentType.Production
#End If

        ConfigManager.SystemID = ConfigurationManager.AppSettings("SYSTEM_ID")
        ConfigManager.SystemName = ConfigurationManager.AppSettings("SYSTEM_NAME")
        ConfigManager.SystemVer = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
        ConfigManager.SystemLogFolder = ConfigurationManager.AppSettings("SYSTEM_LOG_FOLDER")
        ConfigManager.UserId = "Batch"
        ConfigManager.HostName = System.Environment.MachineName

        ConfigManager.Logger = LogFactory.CreateLogManager(ConfigManager.SystemID,
                                                            ConfigManager.SystemName,
                                                            ConfigManager.SystemVer,
                                                            ConfigManager.SystemLogFolder,
                                                            ConfigManager.HostName,
                                                            ConfigManager.UserId)

    End Sub

    ''' <summary>
    ''' ログ開始（ボタン押下時以外は明示的に追加が必要）
    ''' </summary>
    Public Sub StratLog()
        Dim logLevel As LogManager.LogLevel = LogManager.LogLevel.INFO
        sw = Stopwatch.StartNew()

        sw.Start()

        'ログヘッダ登録
        If ConfigManager.Logger.Entry.eventId = Constants.CommonConstants.LOGGER_NOT_STARTED_FLAG Then
            ConfigManager.Logger?.WriteLogHeader(ConfigManager.SystemID, "", ConfigManager.SystemName)
        End If

        'イベント開始
        ConfigManager.Logger?.WriteLog(logLevel, "", $"【{ConfigManager.SystemName}】開始")

    End Sub

    ''' <summary>
    ''' ログ終了（ボタン押下時以外は明示的に追加が必要）
    ''' </summary>
    Public Sub EndLog()
        Dim logLevel As LogManager.LogLevel = LogManager.LogLevel.INFO
        Dim sbLog As New StringBuilder()

        'イベント結果記録
        sbLog.AppendLine($"【{ConfigManager.SystemName}】正常終了")

        sw.Stop()
        sbLog.AppendLine("・実行時間")
        sbLog.AppendLine($"{sw.ElapsedMilliseconds}ms")

        'イベント結果をログに出力
        ConfigManager.Logger?.WriteLog(logLevel, "", sbLog.ToString)
        ConfigManager.Logger?.WriteLog()

    End Sub

    ''' <summary>
    ''' ログ終了※例外発生時（ボタン押下時以外は明示的に追加が必要）
    ''' </summary>
    ''' <param name="errorMsg"></param>
    Public Sub EndLog(errorMsg As String)
        Dim logLevel As LogManager.LogLevel = LogManager.LogLevel.ERROR
        Dim sbLog As New StringBuilder()

        '異常内容記録
        sbLog.AppendLine("・実行結果")
        sbLog.AppendLine($"【{ConfigManager.SystemName}】異常終了")
        sbLog.AppendLine("・異常内容")
        sbLog.AppendLine(errorMsg)

        sw.Stop()
        sbLog.AppendLine("・実行時間")
        sbLog.AppendLine($"{sw.ElapsedMilliseconds}ms")

        'イベント結果をログに出力
        ConfigManager.Logger?.WriteLog(logLevel, "", sbLog.ToString)
        ConfigManager.Logger?.WriteLog()
    End Sub
End Module

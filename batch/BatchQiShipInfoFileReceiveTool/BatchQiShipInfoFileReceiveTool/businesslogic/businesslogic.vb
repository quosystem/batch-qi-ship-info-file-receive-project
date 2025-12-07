Imports System.Configuration
Imports System.Text.RegularExpressions
Imports Amazon.S3
Imports Common
Imports Common.Aws
Imports Common.Log.Manager
Imports Common.Utilities

Public Class BusinessLogic

    Private s3 As S3

    Public Async Function ReceiveFile() As Task(Of Boolean)
        'S3アクセス用のオブジェクトを作成
        SetS3Object()

        ' S3ファイル一覧取得
        Dim keyList As List(Of String) = GetS3FileList()

        If keyList Is Nothing OrElse keyList.Count = 0 Then
            ' 対象ファイルなし
            ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", "受信待ちフォルダ配下にQI出荷情報ファイルが見つかりませんでした。")
            Return True
        End If

        For Each key In keyList
            ' ファイルチェック処理
            Dim ret As Boolean = Await CheckFile(key)
            If ret = False Then
                Continue For
            End If

            ' ファイルダウンロード処理
            Dim localFilePath As String = Await DownLoadFile(key)
            ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ダウンロード成功】{FileUtilities.GetFileNameFromPath(localFilePath)}")

            'ファイル転送処理
            TransferFile(localFilePath)

            ' ダウンロードファイル待避処理
            MoveFile(localFilePath)

            ' S3ファイル削除処理
            Await DeleteDownLoadFile(key)
        Next
        Return True
    End Function

    Private Sub SetS3Object()
        ' S3接続オブジェクト作成
        Dim accessKey As String = ConfigurationManager.AppSettings("S3_ACCESS_KEY")
        Dim secretKey As String = ConfigurationManager.AppSettings("S3_SECRET_KEY")
        Dim region As String = ConfigurationManager.AppSettings("S3_REGION")
        Dim bucketName As String = ConfigurationManager.AppSettings("S3_BUCKET_NAME")

        s3 = New Common.Aws.S3(
                accessKey:=accessKey,
                secretKey:=secretKey,
                region:=region,
                name:=bucketName)

    End Sub

    Private Function GetS3FileList() As List(Of String)
        Dim S3_KEY As String = ConfigurationManager.AppSettings("S3_KEY")
        Dim prefix As String = ConfigurationManager.AppSettings("RECEIVE_FILE_NAME_PREFIX")
        Dim key As String = String.Format(S3_KEY, prefix)

        Return s3.GetAllKeysMatchingPrefix(key, False)
    End Function

    ''' <summary>
    ''' ファイルチェック処理
    ''' </summary>
    ''' <param name="key"></param>
    Private Async Function CheckFile(key As String) As Task(Of Boolean)

        Dim fileName As String = FileUtilities.GetFileNameFromPath(key)

        'ファイル名チェック
        If IsValidQiShipFileName(fileName) = False Then
            ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【フォーマット相違】処理スキップ{vbCrLf}{FileUtilities.GetFileNameFromPath(fileName)}")
            Return False
        End If

        'ダウンロード済ファイルチェック
        If IsDownLoad(fileName) = True Then
            ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ダウンロード済ファイル】削除漏れのため、ファイル削除{vbCrLf}{fileName}")
            Await DeleteDownLoadFile(key)
            Return False
        End If

        Return True
    End Function

    Public Function IsValidQiShipFileName(fileName As String) As Boolean

        ' ファイル名形式：
        ' qi_ship_info_result_SMCxxxxxxxx_yyyymmddhhmmss.csv
        Dim pattern As String = ConfigurationManager.AppSettings("RECEIVE_FILE_NAME_PATTERN")
        Dim match As Match = Regex.Match(fileName, pattern, RegexOptions.IgnoreCase)

        If Not match.Success Then
            ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ファイル名不正】処理スキップ{vbCrLf}{fileName}")
            Return False
        End If

        ' --- ① キー番号（出荷番号） ---
        If IsShKaNo(match.Groups(1).Value) = False Then
            Return False
        End If

        ' --- ② 日付（YYYYMMDDHHMMSS）が妥当か ---
        Dim dtStr As String = match.Groups(2).Value
        Dim dt As DateTime
        If Not DateTime.TryParseExact(dtStr, "yyyyMMddHHmmss",
                                  Nothing,
                                  Globalization.DateTimeStyles.None,
                                  dt) Then
            Return False
        End If

        Return True
    End Function

    ''' <summary>
    ''' 待避フォルダにファイルが存在するかチェックする。
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <returns></returns>
    Private Function IsDownLoad(fileName As String) As Boolean
        Dim bkupFoler As String = ConfigurationManager.AppSettings("BKUP_FOLDER")
        Dim checkFilePath As String = Common.Utilities.FileUtilities.CombinePath(bkupFoler, fileName)

        Return FileUtilities.IsFileExist(checkFilePath)
    End Function


    Private Async Function DownLoadFile(key As String) As Task(Of String)

        ' ダウンロード先フォルダ
        Dim downLoadDir As String = ConfigurationManager.AppSettings("DOWNLOAD_FOLDER")
        ' ダウンロードファイルパス
        Dim localFilePath As String = FileUtilities.CombinePath(downLoadDir, FileUtilities.GetFileNameFromPath(key))

        ' ローカルにファイルダウンロード
        Await s3.DownloadFileAsync(key, localFilePath)

        ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ファイルダウンロード】{vbCrLf}{localFilePath}")
        Return localFilePath
    End Function

    Private Sub TransferFile(filePath As String)

        Dim destFolder As String = ConfigurationManager.AppSettings("DEST_FOLDER")

        FileTransferUtilities.StoreWithHeadFin(
        sourceFilePath:=filePath,
        destFolder:=destFolder)

        ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ファイル転送完了】{FileUtilities.GetFileNameFromPath(filePath)}")
    End Sub

    ''' <summary>
    ''' 待避フォルダにファイルを移動する
    ''' </summary>
    ''' <param name="filePath"></param>
    Private Sub MoveFile(filePath As String)
        ' 処理済みファイル移動処理
        Dim FileName As String = FileUtilities.GetFileNameFromPath(filePath)
        Dim bkupFolder As String = ConfigurationManager.AppSettings("BKUP_FOLDER")
        Dim bkupFilePath As String = FileUtilities.CombinePath(bkupFolder, FileName)

        Common.Utilities.FileUtilities.MoveFile(
                srcPath:=filePath,
                destPath:=bkupFilePath)
        ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【待避フォルダ移動】{vbCrLf}<移動元>{filePath}{vbCrLf}<移動先>{bkupFilePath}")
    End Sub

    Private Async Function DeleteDownLoadFile(key As String) As Task

        ' ファイル削除
        Await s3.DeleteFile(key)
        ConfigManager.Logger?.WriteLog(LogManager.LogLevel.INFO, "", $"【ファイル削除】{vbCrLf}{key}")
    End Function

End Class

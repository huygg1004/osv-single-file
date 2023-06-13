using DataSolutions.ApplicationFramework;
using DataSolutions.Commons.EDI.EdiTools;
using DataSolutions.Commons.Extensions;
using DataSolutions.Commons.FileMovementRetry;
using DataSolutions.DataModels.Xml.ShipNotice;
using DataSolutions.DataModelTools.Xml;
using DataSolutions.Logging.Logger;
using email;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CartersOSVVendorInboundEDI856MappingProcess
{
    public class CartersOSVInboundEDI856Process : DataSolutionsServiceBase
    {
        #region InitVars

        private const string BUYER_SHORT_CODE = "CRR";
        private const string DOCUMENT_CODE = "ASN";
        private const string BOUND = "INBOUND";
        //private const string FILE_EXTENSION_PATTERN = "*.ftp";
        private readonly string FILE_EXTENSION = "edi";
        private readonly string _inputFolder;
        private readonly string _workingFolder;
        private readonly string _outputFolder;
        private readonly string _outputFolder_FA;

        private readonly string _ArchiveFolder;
        private readonly string _ReportFolder;
        private readonly string _xfailedFolder;

        public bool _IsIN_File;
        public List<FileInfo> _IN_INPUT_FILES_LIST;

        private readonly string _fileRetryMovement_Connection_String;

        private readonly string _inputFileExtension;

        private DOC856 _translatedDoc856;

        public static string subject;
        public static string body;
        public static EmailSender sendEmail = new EmailSender();

        #endregion InitVars


        public CartersOSVInboundEDI856Process(Logger logger) : base(logger, Guid.NewGuid())
        {
            BuyerShortCode = BUYER_SHORT_CODE;
            DocumentCode = DOCUMENT_CODE;

            _inputFileExtension = FILE_EXTENSION;


            _inputFolder = ConfigurationManager.AppSettings["CRR_856IN_InputFilePath"];
            _workingFolder = ConfigurationManager.AppSettings["CRR_856IN_WorkingFilePath"];
            _outputFolder = ConfigurationManager.AppSettings["CRR_856IN_OutputFilePath"];
            _outputFolder_FA = ConfigurationManager.AppSettings["CRR_856IN_OutputFilePath_FA"];

            _xfailedFolder = ConfigurationManager.AppSettings["CRR_856IN_FailFilePath"];
            _ArchiveFolder = ConfigurationManager.AppSettings["CRR_856IN_ArchiveFilePath"];
            _ReportFolder = ConfigurationManager.AppSettings["CRR_856IN_ReportedFilePath"];
            _fileRetryMovement_Connection_String = ConfigurationManager.ConnectionStrings["FileRetryMovementDB"].ConnectionString;
        }

        public void DeleteAllFilesInFolders()
        {
            string[] folders = new string[]
            {
                _inputFolder,
                _workingFolder,
                _outputFolder,
                _outputFolder_FA,
                _xfailedFolder,
                _ArchiveFolder,
                _ReportFolder
            };

            foreach (string folder in folders)
            {
                DeleteFilesInFolder(folder);
            }
        }

        // Helper method to delete files in a folder
        private void DeleteFilesInFolder(string folderPath)
        {
            if (Directory.Exists(folderPath))
            {
                string[] files = Directory.GetFiles(folderPath);

                foreach (string file in files)
                {
                    File.Delete(file);
                }
            }
        }


        public class FileTransferRecord
        {
            public long Id { get; set; }
            public string BuyerCode { get; set; }
            public string DocType { get; set; }
            public string SourceFile { get; set; }
            public string DestinationPath { get; set; }
            public string Action { get; set; }
            public DateTime CreatedTime { get; set; }
        }

        public void ResolveFileRetryMovement_Files()
        {
            // Initialize FileMovementRetry instance
            FileMovementRetry FileMovementRetryFunctions = new FileMovementRetry(BuyerShortCode, _fileRetryMovement_Connection_String);

            // Check for existing records with BuyerCode = 'CRR' and Action = 'MOVE'
            bool hasRecords = CheckExistingRecords("CRR", "MOVE");

            // If there are records, perform file movement and continue recursively
            if (hasRecords)
            {
                // Get the records with BuyerCode = 'CRR' and Action = 'MOVE'
                List<FileTransferRecord> records = GetRecordsByBuyerCode("CRR", "MOVE");
                Logger.Info($"Entering ResolveFileRetryMovement_Files... Found {records.Count} records", BuyerShortCode, DocumentCode, CorrelationId);

                foreach (FileTransferRecord record in records)
                {
                    // Check if both SourceFile and DestinationPath are valid paths
                    if (!string.IsNullOrEmpty(record.SourceFile) && !string.IsNullOrEmpty(record.DestinationPath))
                    {
                        // Perform file movement using FileMovementRetryFunctions
                        FileMovementRetryFunctions.MoveToDestination(record.SourceFile, record.DestinationPath, record.DocType, _workingFolder);

                        // Log the success of the file movement
                        Logger.Info($"Moved file from {record.SourceFile} to {record.DestinationPath} successfully.", BuyerShortCode, DocumentCode, CorrelationId);

                        // Delete the processed record from the database
                        DeleteRecord(record.Id);
                    }
                    else
                    {
                        // If either SourceFile or DestinationPath is not valid, quit the function
                        return;
                    }
                }
                // Recursive call to continue resolving remaining records
                ResolveFileRetryMovement_Files();
            }
            Logger.Info("Leaving ResolveFileRetryMovement_Files", BuyerShortCode, DocumentCode, CorrelationId);
        }


        // Helper method to check if a path is valid
        private bool IsValidPath(string path)
        {
            return !string.IsNullOrWhiteSpace(path) && Path.IsPathRooted(path) && File.Exists(path);
        }

        // Helper method to delete a record by its Id
        private void DeleteRecord(long recordId)
        {
            using (SqlConnection connection = new SqlConnection(_fileRetryMovement_Connection_String))
            {
                connection.Open();

                // Execute SQL command to delete the record
                SqlCommand command = new SqlCommand("DELETE FROM [dbo].[FileTransferRecord] WHERE [Id] = @RecordId", connection);
                command.Parameters.AddWithValue("@RecordId", recordId);

                command.ExecuteNonQuery();
            }
        }

        // Helper method to check if there are existing records with a given BuyerCode and Action
        private bool CheckExistingRecords(string buyerCode, string action)
        {
            using (SqlConnection connection = new SqlConnection(_fileRetryMovement_Connection_String))
            {
                connection.Open();

                // Execute SQL query to check for records
                SqlCommand command = new SqlCommand("SELECT COUNT(*) FROM [dbo].[FileTransferRecord] WHERE [BuyerCode] = @BuyerCode AND [Action] = @Action", connection);
                command.Parameters.AddWithValue("@BuyerCode", buyerCode);
                command.Parameters.AddWithValue("@Action", action);

                int count = (int)command.ExecuteScalar();

                return count > 0;
            }
        }

        // Helper method to retrieve records with a given BuyerCode and Action
        private List<FileTransferRecord> GetRecordsByBuyerCode(string buyerCode, string action)
        {
            List<FileTransferRecord> records = new List<FileTransferRecord>();

            using (SqlConnection connection = new SqlConnection(_fileRetryMovement_Connection_String))
            {
                connection.Open();

                // Execute SQL query to retrieve records
                SqlCommand command = new SqlCommand("SELECT * FROM [dbo].[FileTransferRecord] WHERE [BuyerCode] = @BuyerCode AND [Action] = @Action", connection);
                command.Parameters.AddWithValue("@BuyerCode", buyerCode);
                command.Parameters.AddWithValue("@Action", action);

                SqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    FileTransferRecord record = new FileTransferRecord()
                    {
                        Id = (long)reader["Id"],
                        BuyerCode = (string)reader["BuyerCode"],
                        DocType = (string)reader["DocType"],
                        SourceFile = (string)reader["SourceFile"],
                        DestinationPath = (string)reader["DestinationPath"],
                        Action = (string)reader["Action"],
                        CreatedTime = (DateTime)reader["CreatedTime"]
                    };

                    records.Add(record);
                }

                reader.Close();
            }

            return records;
        }



        public void DoWork()
        {
            //DeleteAllFilesInFolders();

            //Try to clear any records in File Retry movement Db
            ResolveFileRetryMovement_Files();

            int filenumberinLoop = 0;
            Logger.Info("Entering CartersOSVVendorInboundEDI856MappingProcess.CartersOSVInboundEDI856Process.DoWork...", BuyerShortCode, DocumentCode, CorrelationId);
            FileMovementRetry FileMovementRetryFunctions = new FileMovementRetry(BuyerShortCode, _fileRetryMovement_Connection_String);

            try
            {
                #region handlingINFiles

                List<FileInfo> inputINfiles = new List<FileInfo>();
                var inputFiles = (new DirectoryInfo(_inputFolder)).GetFiles("*.*").ToList();

                foreach (FileInfo file in inputFiles)
                {
                    if (file.Extension.Equals(".in", StringComparison.OrdinalIgnoreCase))
                    {
                        // Create a new file name with the ".edi" extension
                        string newFileName = Path.ChangeExtension(file.FullName, ".edi");

                        // Copy the file to a new path with the ".edi" extension
                        File.Copy(file.FullName, newFileName);

                        _IsIN_File = true;
                    }
                }

                inputFiles = (new DirectoryInfo(_inputFolder)).GetFiles("*.edi").ToList();
                if (_IsIN_File)
                {
                    _IN_INPUT_FILES_LIST = (new DirectoryInfo(_inputFolder)).GetFiles("*.in").ToList();
                }

                #endregion handlingINFiles


                int IN_FILE_COUNT = 0;

                foreach (FileInfo fl in inputFiles)
                {
                    filenumberinLoop++;
                    

                    #region directory initialization
                    string edifilenameWithExtension = Path.GetFileName(fl.FullName);
                    string edifilenameWithoutExtension = Path.GetFileNameWithoutExtension(fl.FullName);


                    string filePathWorking = Path.Combine(_workingFolder, edifilenameWithoutExtension);
                    string filePathInput = Path.Combine(_inputFolder, edifilenameWithExtension);
                    string filePathArchive = Path.Combine(_ArchiveFolder, edifilenameWithoutExtension);
                    string filePathOutput = Path.Combine(_outputFolder, edifilenameWithoutExtension);
                    string filePathOutput_FA = Path.Combine(_outputFolder_FA, edifilenameWithoutExtension);
                    #endregion

                    try
                    {
                        #region Generating DOC856
                        Logger.Info("Processing File: " + filePathInput, BuyerShortCode, DocumentCode, CorrelationId);
                        //get edi data
                        var ediData = File.ReadAllText(filePathInput);
                        EdiSerializer ediSerializer = new EdiSerializer();
                        EdiDocument ediDocument = ediSerializer.Deserialise(ediData);
                        EdiDocument ediDocument2 = ediSerializer.Deserialise(ediData);
                        EdiDocument ediDocumentO = ediSerializer.Deserialise(ediData);

                        #region generating 997 output edi file
                        Logger.Info("Generating EDI997 file", BuyerShortCode, CorrelationId);

                        List<EdiSegment> Document_997EDI = new List<EdiSegment>();


                        // finding segment delimiter and the composite delimiter 
                        string segment_delimiter = "*";
                        string composite_delimiter = "~";

                        //Counting ST loops and extracting ST values from EDI856 document
                        var ST_Count = ediDocument.Segments.Where(x => x.Name == "ST").Count();
                        var ST_loops_List = ediDocument.Segments.Where(x => x.Name == "ST");

                        #region ISA 997 segment
                        EdiSegment ediSegment_ISA = new EdiSegment();
                        ediSegment_ISA.Name = "ISA";
                        ediSegment_ISA.OrderInDocument = 0;

                        ediSegment_ISA.Values = new List<string>();

                        ediSegment_ISA.Values.Add("ISA");
                        ediSegment_ISA.Values.Add("00"); //ISA01
                        ediSegment_ISA.Values.Add(new string(' ', 10)); //ISA02
                        ediSegment_ISA.Values.Add("00"); //ISA03
                        ediSegment_ISA.Values.Add(new string(' ', 10)); //ISA04
                        ediSegment_ISA.Values.Add(GetSegmentValue(ediDocument, "ISA", 7)); //ISA05
                        //ISA06
                        if ((GetSegmentValue(ediDocument, "ISA", 8).Length < 15))
                        {
                            string add_value = GetSegmentValue(ediDocument, "ISA", 8) + new string(' ', 15 - GetSegmentValue(ediDocument, "ISA", 8).Length);
                            ediSegment_ISA.Values.Add(add_value);
                        }
                        else
                        {
                            ediSegment_ISA.Values.Add(GetSegmentValue(ediDocument, "ISA", 8));
                        }

                        ediSegment_ISA.Values.Add(GetSegmentValue(ediDocument, "ISA", 5)); //ISA07

                        //ISA08
                        if ((GetSegmentValue(ediDocument, "ISA", 6).Length < 15))
                        {
                            string add_value = GetSegmentValue(ediDocument, "ISA", 6) + new string(' ', 15 - GetSegmentValue(ediDocument, "ISA", 6).Length);
                            ediSegment_ISA.Values.Add(add_value);
                        }
                        else
                        {
                            ediSegment_ISA.Values.Add(GetSegmentValue(ediDocument, "ISA", 6));
                        }

                        //ISA09
                        ediSegment_ISA.Values.Add(string.Format("{0:yyMMdd}", DateTime.Now));

                        //ISA10
                        ediSegment_ISA.Values.Add(DateTime.Now.ToString("HHmm"));

                        //ISA11
                        ediSegment_ISA.Values.Add("U");

                        //ISA12
                        ediSegment_ISA.Values.Add("00401");

                        //ISA13
                        ediSegment_ISA.Values.Add(filenumberinLoop.ToString().PadLeft(9, '0'));

                        //ISA14
                        ediSegment_ISA.Values.Add("0");

                        //ISA15
                        ediSegment_ISA.Values.Add("T");

                        //ISA16
                        ediSegment_ISA.Values.Add(">");

                        #endregion

                        #region GS segment

                        EdiSegment ediSegment_GS = new EdiSegment();
                        ediSegment_GS.Name = "GS";
                        ediSegment_GS.OrderInDocument = 1;
                        ediSegment_GS.Values = new List<string>();
                        ediSegment_GS.Values.Add("GS");
                        //GS01
                        ediSegment_GS.Values.Add("FA");

                        //GS02
                        ediSegment_GS.Values.Add(GetSegmentValue(ediDocument, "ISA", 8));

                        //GS03
                        ediSegment_GS.Values.Add(GetSegmentValue(ediDocument, "ISA", 6));

                        //GS04
                        ediSegment_GS.Values.Add(DateTime.Now.ToString("yyyyMMdd"));

                        //GS05
                        ediSegment_GS.Values.Add(DateTime.Now.ToString("HHmm"));

                        //GS06
                        ediSegment_GS.Values.Add(filenumberinLoop.ToString());

                        //GS07
                        ediSegment_GS.Values.Add("X");

                        //GS08
                        ediSegment_GS.Values.Add("004010VICS");
                        #endregion

                        #region ST segment
                        EdiSegment ediSegment_ST = new EdiSegment();
                        ediSegment_ST.Name = "ST";
                        ediSegment_ST.OrderInDocument = 2;
                        ediSegment_ST.Values = new List<string>();
                        ediSegment_ST.Values.Add("ST");

                        //ST01
                        ediSegment_ST.Values.Add("997");

                        //ST02
                        ediSegment_ST.Values.Add("0001");

                        #endregion

                        #region AK segments- AK1 -- AK2 -- AK5 -- AK2 -- AK5 -- AK9
                        //AK1
                        EdiSegment ediSegment_AK1 = new EdiSegment();
                        ediSegment_AK1.Name = "AK1";
                        ediSegment_AK1.OrderInDocument = 3;
                        ediSegment_AK1.Values = new List<string>();
                        ediSegment_AK1.Values.Add("AK1");
                        //AK101
                        ediSegment_AK1.Values.Add("SH");
                        //AK102
                        ediSegment_AK1.Values.Add(GetSegmentValue(ediDocument, "GS", 6));


                        //Creating AK02 and AK05 as a group matching with number of ST loops count
                        List<EdiSegment> AK2_AK5_list = new List<EdiSegment>();
                        for (int i = 0; i < ST_Count; i++)
                        {
                            Console.WriteLine(i);
                            //AK2
                            EdiSegment ediSegment_AK2 = new EdiSegment();
                            ediSegment_AK2.Name = "AK2";
                            ediSegment_AK2.OrderInDocument = 4;
                            ediSegment_AK2.Values = new List<string>();
                            ediSegment_AK2.Values.Add("AK2");
                            //AK201
                            ediSegment_AK2.Values.Add("856");
                            //AK202
                            ediSegment_AK2.Values.Add(GetSegmentValue(ediDocument, "ST", 2));
                            AK2_AK5_list.Add(ediSegment_AK2);

                            //AK5
                            EdiSegment ediSegment_AK5 = new EdiSegment();
                            ediSegment_AK5.Name = "AK5";
                            ediSegment_AK5.OrderInDocument = 5;
                            ediSegment_AK5.Values = new List<string>();
                            ediSegment_AK5.Values.Add("AK5");
                            //AK501
                            ediSegment_AK5.Values.Add("A");
                            AK2_AK5_list.Add(ediSegment_AK5);
                        }


                        //AK9
                        EdiSegment ediSegment_AK9 = new EdiSegment();
                        ediSegment_AK9.Name = "AK9";
                        ediSegment_AK9.OrderInDocument = 6;
                        ediSegment_AK9.Values = new List<string>();
                        ediSegment_AK9.Values.Add("AK9");
                        //AK901
                        ediSegment_AK9.Values.Add("A");

                        //AK902
                        ediSegment_AK9.Values.Add(ST_Count.ToString());
                        ediSegment_AK9.Values.Add(ST_Count.ToString());
                        ediSegment_AK9.Values.Add(ST_Count.ToString());

                        #endregion

                        #region SE segments
                        //SE
                        EdiSegment ediSegment_SE = new EdiSegment();
                        ediSegment_SE.Name = "SE";
                        ediSegment_SE.OrderInDocument = 7;
                        ediSegment_SE.Values = new List<string>();
                        ediSegment_SE.Values.Add("SE");

                        //SE01
                        var TEST = ediDocument.Segments.Count();
                        ediSegment_SE.Values.Add((ediDocument.Segments.Count() - 4).ToString());

                        //SE02
                        ediSegment_SE.Values.Add("0001");

                        #endregion

                        #region GE segments
                        //GE
                        EdiSegment ediSegment_GE = new EdiSegment();
                        ediSegment_GE.Name = "GE";
                        ediSegment_GE.OrderInDocument = 8;
                        ediSegment_GE.Values = new List<string>();
                        ediSegment_GE.Values.Add("GE");
                        //GE01
                        ediSegment_GE.Values.Add("1");
                        //GE02
                        ediSegment_GE.Values.Add(filenumberinLoop.ToString());

                        #endregion

                        #region IEA segment
                        //IEA
                        EdiSegment ediSegment_IEA = new EdiSegment();
                        ediSegment_IEA.Name = "IEA";
                        ediSegment_IEA.OrderInDocument = 9;
                        ediSegment_IEA.Values = new List<string>();
                        ediSegment_IEA.Values.Add("IEA");
                        //IEA01
                        ediSegment_IEA.Values.Add("1");
                        //IEA02
                        ediSegment_IEA.Values.Add(filenumberinLoop.ToString().PadLeft(9, '0'));

                        #endregion

                        #region output EDI997


                        Document_997EDI.AddRange(new List<EdiSegment>() { ediSegment_ISA, ediSegment_GS, ediSegment_ST, ediSegment_AK1 });
                        foreach (var segment in AK2_AK5_list)
                        {
                            Document_997EDI.Add(segment);
                        }
                        Document_997EDI.AddRange(new List<EdiSegment>() { ediSegment_AK9, ediSegment_SE, ediSegment_GE, ediSegment_IEA });

                        EdiDocument Final_Output_EDI997 = new EdiDocument(Document_997EDI, '*', 'U', '>', "~");

                        Logger.Info("Finish generating EDI997 file, output EDI997 file can be found at " + _outputFolder_FA.ToString(), BuyerShortCode, CorrelationId);
                        var output997 = ediSerializer.Serialise(Final_Output_EDI997);
                        string ediFileName = "FA" + fl.ToString();
                        File.WriteAllText(Path.Combine(_outputFolder_FA, ediFileName), output997);

                        #endregion


                        #endregion


                        _translatedDoc856 = GenerateXML856(_workingFolder, edifilenameWithoutExtension, ediDocument, ediDocument2, ediDocumentO);

                        var tempOutputFileName = Path.Combine(_workingFolder
                                          , edifilenameWithoutExtension + ".xml");

                        var finalOutputFileName = Path.Combine(_outputFolder
                                          , edifilenameWithoutExtension + ".xml");

                        var archiveOutputFileName = Path.Combine(_ArchiveFolder
                                          , edifilenameWithoutExtension + ".xml");
                        #endregion


                        #region try outputing DOC856 XML - input EDI moved to archive
                        // Output DOC856 Xml
                        try
                        {
                            var xml856Serializer = new DocumentFileSerializer<DOC856>(Logger, CorrelationId);

                            //Output XML856 file will be generated at Working file path
                            xml856Serializer.SerializeToFile(_translatedDoc856
                                                               , tempOutputFileName);

                            //the original EDI856 file will be moved from Input file path to Archive file path for archive purpose
                            if (!_IsIN_File)
                            {
                                try
                                {
                                    bool isMoveSuccess = FileMovementRetryFunctions.MoveToArchiveOutputFile(filePathInput, _ArchiveFolder, "EDI856", _workingFolder);
                                    string fileName = Path.GetFileName(filePathInput);
                                    string logMessage = isMoveSuccess ? "File move success" : "Failed to move file";
                                    Logger.Error($"{logMessage}: {fileName}, destination path: {_ArchiveFolder}", BuyerShortCode, DocumentCode, CorrelationId);
                                }
                                catch (Exception ex)
                                {
                                    Logger.Error("Original EDI856 file cannot be archived." + ex.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                                }
                            }
                            else
                            {
                                File.Delete(filePathInput);
                                bool isMoveSuccess = FileMovementRetryFunctions.MoveToArchiveOutputFile(_IN_INPUT_FILES_LIST[IN_FILE_COUNT].FullName, _ArchiveFolder, "IN", _workingFolder);
                                string fileName = Path.GetFileName(_IN_INPUT_FILES_LIST[IN_FILE_COUNT].FullName);
                                string logMessage = isMoveSuccess ? "File move success" : "Failed to move file";
                                Logger.Error($"{logMessage}: {fileName}, destination path: {_ArchiveFolder}", BuyerShortCode, DocumentCode, CorrelationId);
                            }

                        }
                        catch (Exception ex)
                        {
                            Logger.Error("xml856 cannot be generated." + ex.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                        }
                        #endregion


                        #region output copy from tmp to output and then moved to archive
                        //Output XML856 file will be copied from Working file path to Output file path.
                        try
                        {
                            FileMovementRetryFunctions.CopyToDestination(filePathWorking + ".xml", filePathOutput + ".xml", "XML856", _workingFolder);
                            Logger.Info("XML file is copied to Output folder. Path: " + filePathOutput + ".xml", BuyerShortCode, DocumentCode, CorrelationId);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error(" output XML856 file cannot be copied." + ex.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                        }

                        //Output XML856 file will be moved from Working file path to Archive file path.
                        try
                        {
                            bool isMoveSuccess = FileMovementRetryFunctions.MoveToArchiveOutputFile(filePathWorking + ".xml", _ArchiveFolder, "XML856", _workingFolder);
                            string fileName = Path.GetFileName(filePathWorking + ".xml");
                            string logMessage = isMoveSuccess ? "File move success" : "Failed to move file";
                            Logger.Error($"{logMessage}: {fileName}, destination path: {_ArchiveFolder}", BuyerShortCode, DocumentCode, CorrelationId);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Output XML856 file cannot be archived from tmp." + ex.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                        }
                        #endregion

                        Logger.Info("XML file was generated at Output folder. Path: " + filePathOutput, BuyerShortCode, DocumentCode, CorrelationId);

                    }
                    catch (Exception ex)
                    {
                        string fileFailedPath = Path.Combine(_xfailedFolder, edifilenameWithExtension);
                        #region if edi file fail move to xfailed
                        //if input edi file is wrong, then move that file to xfailed - edi or in
                        //EDI CASE
                        if (!_IsIN_File)
                        {
                            try
                            {
                                //get the current edi file names being processed;
                                var EDI_FILE_NAME = Path.GetFileName(filePathInput);
                                var EDI_FILE_FULL_PATH_NAME = Path.GetFileName(filePathInput);

                                //Log that file contains error:
                                Logger.Info(EDI_FILE_NAME + " failed to be processed and will be moved to xfailed folder", BuyerShortCode, DocumentCode, CorrelationId);
                                Logger.Error("Exception Message: ", ex, BuyerShortCode, DocumentCode, CorrelationId, EDI_FILE_NAME, 0);
                                //If email success = 1 -> reported folder, =0 --> regular xfailed folder
                                if (EmailAlerts("", EDI_FILE_NAME, EDI_FILE_FULL_PATH_NAME))
                                {
                                    FileMovementRetryFunctions.MoveToDestination(EDI_FILE_FULL_PATH_NAME, Path.Combine(_ReportFolder, EDI_FILE_NAME), "EDI856", _workingFolder);
                                }
                                else
                                {
                                    FileMovementRetryFunctions.MoveToDestination(EDI_FILE_FULL_PATH_NAME, Path.Combine(_xfailedFolder, EDI_FILE_NAME), "EDI856", _workingFolder);
                                }
                            }
                            catch (Exception ex_mess)
                            {
                                Logger.Error(" input EDI856 file cannot be moved." + ex_mess.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                            }
                        } else
                        // IN CASE
                        {
                            try
                            {
                                //Get the current file being process:
                                var current_input_IN_file = _IN_INPUT_FILES_LIST[IN_FILE_COUNT];
                                //get current in file being processed with extension:
                                var IN_FILE_NAME = current_input_IN_file.Name;
                                var IN_FILE_FULL_PATH_NAME = current_input_IN_file.FullName;

                                //Log that file contains error:
                                Logger.Info(_IN_INPUT_FILES_LIST[IN_FILE_COUNT].Name + " failed to be processed and will be moved to xfailed folder", BuyerShortCode, DocumentCode, CorrelationId);
                                Logger.Error("Exception Message: ", ex, BuyerShortCode, DocumentCode, CorrelationId, IN_FILE_NAME, 0);

                                //If email success = 1 -> reported folder, =0 --> regular xfailed folder
                                if (EmailAlerts("", IN_FILE_NAME, IN_FILE_FULL_PATH_NAME))
                                {
                                    //Try to delete any remaining edi file
                                    string[] ediFiles = Directory.GetFiles(_inputFolder, "*.edi");
                                    foreach (string ediFile in ediFiles)
                                    {
                                        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(ediFile);
                                        if (fileNameWithoutExtension == Path.GetFileNameWithoutExtension(IN_FILE_NAME))
                                        {
                                            // Match found, delete the .edi file
                                            File.Delete(ediFile);
                                            Console.WriteLine("Deleted file: " + ediFile);
                                            break;
                                        }
                                    }

                                    bool IsMoveSuccess = FileMovementRetryFunctions.MoveToDestination(IN_FILE_FULL_PATH_NAME, Path.Combine(_ReportFolder, IN_FILE_NAME),"IN", _workingFolder);
                                    string logMessage = IsMoveSuccess ? IN_FILE_NAME + " is moved successfully to destination xfailed/reported folder." : IN_FILE_NAME + " is NOT moved successfully to destination xfailed/reported folder, will be in tmp folder for process next cycle.";
                                    Logger.Info(logMessage, BuyerShortCode, DocumentCode, CorrelationId);
                                }
                                else
                                {
                                    //Try to delete any remaining edi file
                                    string[] ediFiles = Directory.GetFiles(_inputFolder, "*.edi");
                                    foreach (string ediFile in ediFiles)
                                    {
                                        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(ediFile);
                                        if (fileNameWithoutExtension == Path.GetFileNameWithoutExtension(IN_FILE_NAME))
                                        {
                                            // Match found, delete the .edi file
                                            File.Delete(ediFile);
                                            Console.WriteLine("Deleted file: " + ediFile);
                                            break;
                                        }
                                    }


                                    bool IsMoveSuccess = FileMovementRetryFunctions.MoveToDestination(IN_FILE_FULL_PATH_NAME, Path.Combine(_xfailedFolder, IN_FILE_NAME), "IN", _workingFolder);
                                    string logMessage = IsMoveSuccess ? IN_FILE_NAME+ " is moved successfully to destination xfailed folder." : IN_FILE_NAME + " is NOT moved successfully to destination xfailed folder, will be in tmp folder for process next cycle.";
                                    Logger.Info(logMessage, BuyerShortCode, DocumentCode, CorrelationId);

                                }


                                //string failed_IN_filename = Path.GetFileName(_IN_INPUT_FILES_LIST[IN_FILE_COUNT].FullName);
                                //fileFailedPath = Path.Combine(_xfailedFolder, failed_IN_filename);

                                //FileMovementRetryFunctions.MoveToDestination(_IN_INPUT_FILES_LIST[IN_FILE_COUNT].FullName, fileFailedPath, 1, "IN");
                                //File.Delete(filePathInput);
                                //Logger.Info("EDI856 file is moved to destination xfailed folder. Path: " + filePathOutput + ".xml", BuyerShortCode, DocumentCode, CorrelationId);
                            }
                            catch (Exception ex_mess)
                            {
                                Logger.Error(" input EDI856 file cannot be moved." + ex_mess.ToString(), BuyerShortCode, DocumentCode, CorrelationId);
                            }

                        }


                        #endregion
                        Logger.Error("Program failed.", BuyerShortCode, DocumentCode, CorrelationId);
                    }

                    IN_FILE_COUNT++;
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message, BuyerShortCode, DocumentCode, CorrelationId);
            }
            Logger.Info("Leaving CartersOSVVendorInboundEDI856MappingProcess.CartersOSVInboundEDI856Process.DoWork...", BuyerShortCode, DocumentCode, CorrelationId);

        }
        public DOC856 GenerateXML856(string outputPath, string ediFileName, EdiDocument ediDocument, EdiDocument ediDocument2, EdiDocument ediDocumentO)
        {
            //Initialize O level
            int MainLoop_O_Level = 0;

            Logger.Info("Entered CartersOSVVendorInboundEDI856MappingProcess.GenerateXML856()", BuyerShortCode, CorrelationId);
            DOC856 translatedDoc856 = new DOC856();
            #region Translating DOC856 TRY
            try
            {
                #region Header
                int currentHierarchyId = 0;
                int currentShipLevelId = 0;
                int currentOrderLevelId = 0;
                int currentPackLevelId = 0;
                int currentItemLevelId = 0;

                int PARENT_ID_FOR_I_LEVEL_EQUAL_P_LEVEL_HIREID = 0;

                translatedDoc856.SENDER = ediDocument.Segments[11].Values[2];
                translatedDoc856.RECEIVER = "CARTERS";

                translatedDoc856.HEADER = new DataSolutions.DataModels.Xml.ShipNotice.HEADER();

                try { translatedDoc856.HEADER.DOCPURPOSE = GetSegmentValue(ediDocument, "BSN", 1); } catch (Exception) { translatedDoc856.HEADER.DOCPURPOSE = ""; };
                try { translatedDoc856.HEADER.ASNNUM = GetSegmentValue(ediDocument, "BSN", 2); } catch (Exception) { translatedDoc856.HEADER.ASNNUM = ""; };
                try { translatedDoc856.HEADER.CREATEDT = GetSegmentValue(ediDocument, "BSN", 3); } catch (Exception) { translatedDoc856.HEADER.CREATEDT = ""; };
                try { translatedDoc856.HEADER.CREATETM = GetSegmentValue(ediDocument, "BSN", 4); } catch (Exception) { translatedDoc856.HEADER.CREATETM = ""; };
                try { translatedDoc856.HEADER.HIERSTRUC = GetSegmentValue(ediDocument, "BSN", 5); } catch (Exception) { translatedDoc856.HEADER.HIERSTRUC = ""; };

                List<DataSolutions.DataModels.Xml.ShipNotice.DETAIL> asnDetails = new List<DataSolutions.DataModels.Xml.ShipNotice.DETAIL>();
                int CartonSequence = 1;

                #endregion

                Dictionary<int, int> Olevel_and_PQuantity = new Dictionary<int, int>();
                int current_O_level_iteration = 0;

                for (int i = 1; i < ediDocument.Segments.Count(); i++)
                {
                    if (ediDocument.Segments[i].Name == "HL")
                    {
                        if (ediDocument.Segments[i].Values[3] == "O")
                        {
                            current_O_level_iteration++;
                            i++;
                            while (ediDocument.Segments[i].Name != "HL") { i++; }
                            i--;
                        }
                        else if ((ediDocument.Segments[i].Values[3] == "P"))
                        {
                            int ItemQuantitySum = 0;
                            int m = i;
                            if (ediDocument.Segments[m].Name == "HL")
                            {
                                if (ediDocument.Segments[m].Values[3] == "P")
                                {
                                    m++;
                                    while (ediDocument.Segments[m].Values[3] != "I" && ediDocument.Segments[m].Name != "HL") { m++; }
                                    while (ediDocument.Segments[m].Values[3] != "P" && ediDocument.Segments[m].Values[3] == "I")
                                    {
                                        ItemQuantitySum = ItemQuantitySum + Int32.Parse(ediDocument.Segments[m + 2].Values[2]);
                                        m += 4;
                                        try { if (ediDocument.Segments[m].Values[3] != "I") { break; } } catch (Exception) { break; }
                                    }
                                }
                            }

                            bool keyExists = Olevel_and_PQuantity.ContainsKey(current_O_level_iteration);
                            if (keyExists)
                            {
                                Olevel_and_PQuantity[current_O_level_iteration] = Olevel_and_PQuantity[current_O_level_iteration] + ItemQuantitySum;
                            }
                            else
                            {
                                Olevel_and_PQuantity.Add(current_O_level_iteration, ItemQuantitySum);
                            }

                            i++;
                            while (ediDocument.Segments[i].Name != "HL") { i++; }
                            i--;
                        }
                    }
                }

                current_O_level_iteration = 0;
                //for (int i = 4; i < ediDocument.Segments.Count(); i++)
                for (int i = 4; i < ediDocument.Segments.Count(); i++)
                {
                    //Looping and checking if segment name is HL

                    if (ediDocument.Segments[i].Name == "HL")
                    {
                        #region S Level
                        //Handling Shiplevel Segments
                        if (ediDocument.Segments[i].Values[3] == "S")
                        {
                            //Increment IDs
                            currentHierarchyId++;
                            currentShipLevelId = currentHierarchyId;

                            //Initiate new Ship Level
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL shipLevel = new DataSolutions.DataModels.Xml.ShipNotice.DETAIL();

                            //Constructing Ship Level Detail

                            shipLevel.HIERARCHY = new HIERARCHY { HIERID = currentHierarchyId.ToString(), PARENT = "0", LEVELTYPE = "S" };
                            try { shipLevel.PACKTYPE = ediDocument.Segments[i + 1].Values[1]; } catch (Exception) { shipLevel.PACKTYPE = ""; }
                            try { shipLevel.QUANTITY = ediDocument.Segments[i + 1].Values[2]; } catch (Exception) { shipLevel.QUANTITY = ""; }


                            //NetWeightUOM vs GrossWeightUOM
                            if (ediDocument.Segments[i + 1].Values[6] == "G")
                            {
                                try { shipLevel.WEIGHT = ediDocument.Segments[i + 1].Values[7]; } catch (Exception) { shipLevel.WEIGHT = ""; }
                                string grossWeightUOM_Extra_CONTENT = "";
                                try { grossWeightUOM_Extra_CONTENT = ediDocument.Segments[i + 1].Values[8]; } catch (Exception) { grossWeightUOM_Extra_CONTENT = ""; }
                                DataSolutions.DataModels.Xml.ShipNotice.EXTRA grossWeightUOM_Extra = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "999GWUOM", LABEL = "Gross Weight UOM", CONTENT = grossWeightUOM_Extra_CONTENT };
                                shipLevel.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { grossWeightUOM_Extra }.ToArray();
                            }
                            else if (ediDocument.Segments[i + 1].Values[6] == "N")
                            {
                                try { shipLevel.NETWGHT = ediDocument.Segments[i + 1].Values[7]; } catch (Exception) { shipLevel.NETWGHT = ""; }
                                string CONTENT_netWeightUOM_Extra = "";
                                try { CONTENT_netWeightUOM_Extra = ediDocument.Segments[i + 1].Values[8]; } catch (Exception) { CONTENT_netWeightUOM_Extra = ""; }
                                DataSolutions.DataModels.Xml.ShipNotice.EXTRA netWeightUOM_Extra = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "999NWUOM", LABEL = "Net Weight UOM", CONTENT = CONTENT_netWeightUOM_Extra };
                                shipLevel.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { netWeightUOM_Extra }.ToArray();
                            }

                            try { shipLevel.VOLUME = ediDocument.Segments[i + 1].Values[9]; } catch (Exception) { shipLevel.VOLUME = ""; }
                            try { shipLevel.VOLUMEUOM = ediDocument.Segments[i + 1].Values[10]; } catch (Exception) { shipLevel.VOLUMEUOM = ""; }
                            try { shipLevel.SCAC = ediDocument.Segments[i + 2].Values[3]; } catch (Exception) { shipLevel.SCAC = ""; }
                            try { shipLevel.SHIPMODE = ediDocument.Segments[i + 2].Values[4]; } catch (Exception) { shipLevel.SHIPMODE = ""; }
                            try { shipLevel.VESSEL = ediDocument.Segments[i + 2].Values[5]; } catch { }
                            try { shipLevel.LADINGDESC = ediDocument.Segments[i + 4].Values[1]; } catch (Exception) { shipLevel.LADINGDESC = ""; }
                            try { shipLevel.CNTNRPRE = ediDocument.Segments[i + 4].Values[2]; } catch (Exception) { shipLevel.CNTNRPRE = ""; }
                            try { shipLevel.CNTNRNUM = ediDocument.Segments[i + 4].Values[3]; } catch (Exception) { shipLevel.CNTNRNUM = ""; }
                            try { shipLevel.SEALNUM = ediDocument.Segments[i + 4].Values[9]; } catch { }

                            //BM REFNUM
                            string CONTENT_SHIPREFNUM_BM = "";
                            try { CONTENT_SHIPREFNUM_BM = ediDocument.Segments[i + 5].Values[2]; } catch (Exception) { CONTENT_SHIPREFNUM_BM = ""; }
                            SHIPREFNUM SHIPREFNUM_BM = new SHIPREFNUM { NUMTYPE = "BM", REFNUM = CONTENT_SHIPREFNUM_BM };

                            //2L REFNUM
                            string CONTENT_SHIPREFNUM_2L = "";
                            try { CONTENT_SHIPREFNUM_2L = ediDocument.Segments[i + 6].Values[2]; } catch (Exception) { CONTENT_SHIPREFNUM_2L = ""; }
                            SHIPREFNUM SHIPREFNUM_2L = new SHIPREFNUM { NUMTYPE = "2L", REFNUM = CONTENT_SHIPREFNUM_2L };

                            //IA REFNUM
                            string CONTENT_SHIPREFNUM_IA = "";
                            try { CONTENT_SHIPREFNUM_IA = ediDocument.Segments[i + 7].Values[2]; } catch (Exception) { CONTENT_SHIPREFNUM_IA = ""; }
                            SHIPREFNUM SHIPREFNUM_IA = new SHIPREFNUM { NUMTYPE = "IA", REFNUM = CONTENT_SHIPREFNUM_IA };

                            //Add to Shiplevel SHIPREFNUM
                            shipLevel.SHIPREFNUM = new List<SHIPREFNUM> { SHIPREFNUM_BM, SHIPREFNUM_2L, SHIPREFNUM_IA }.ToArray();


                            //Date Infos for Shiplevel:
                            string CONTENT_dateInfo_067 = "";
                            try
                            {
                                CONTENT_dateInfo_067 = ediDocument.Segments[i + 8].Values[2];
                                if (CONTENT_dateInfo_067.Length > 8)
                                {
                                    CONTENT_dateInfo_067 = ediDocument.Segments[i + 8].Values[2].Substring(0, 8);
                                }
                            }
                            catch (Exception)
                            {
                                CONTENT_dateInfo_067 = "";
                            }
                            DataSolutions.DataModels.Xml.ShipNotice.DATEINFO dateInfo_067 = new DataSolutions.DataModels.Xml.ShipNotice.DATEINFO { DATETYPE = "017", DATE = CONTENT_dateInfo_067 };

                            string CONTENT_dateInfo_068 = "";
                            for (int dateDTM068_iter = i + 9; dateDTM068_iter < 16; dateDTM068_iter++)
                            {
                                if (ediDocument.Segments[dateDTM068_iter].Name == "DTM" && ediDocument.Segments[dateDTM068_iter].Values[1] == "068")
                                {
                                    try
                                    {
                                        CONTENT_dateInfo_068 = ediDocument.Segments[dateDTM068_iter].Values[2];
                                    }
                                    catch (Exception)
                                    {
                                        CONTENT_dateInfo_068 = "";
                                    }
                                }
                            }

                            if (ediDocument.Segments[12].Values.Count > 3)
                            {
                                CONTENT_dateInfo_068 = ediDocument.Segments[12].Values[4].Substring(0, 8);
                            }

                            DataSolutions.DataModels.Xml.ShipNotice.DATEINFO dateInfo_068 = new DataSolutions.DataModels.Xml.ShipNotice.DATEINFO { DATETYPE = "002", DATE = CONTENT_dateInfo_068 };

                            //Add to Shiplevel Dateinfos
                            if (CONTENT_dateInfo_068 == "")
                            {
                                shipLevel.DATEINFO = new List<DataSolutions.DataModels.Xml.ShipNotice.DATEINFO> { dateInfo_067 }.ToArray();
                            }
                            else
                            {
                                shipLevel.DATEINFO = new List<DataSolutions.DataModels.Xml.ShipNotice.DATEINFO> { dateInfo_067, dateInfo_068 }.ToArray();
                            }


                            var AbbreviateEDIDocument = ediDocument2;
                            int elementsToDelete = ediDocument2.Segments.Count - 16;
                            AbbreviateEDIDocument.Segments.RemoveRange(ediDocument2.Segments.Count - elementsToDelete, elementsToDelete);



                            //Handling Shiploc
                            string LOCTYPE, NAME, ID, ADDRLINE1, CITY, PROVSTATE, POSTALCODE, COUNTRY;
                            LOCTYPE = NAME = ID = ADDRLINE1 = CITY = PROVSTATE = POSTALCODE = COUNTRY = "";
                            LOCTYPE = "ST";

                            try { NAME = GetSegmentValue(AbbreviateEDIDocument, "N1", 2); } catch (Exception) { NAME = ""; }
                            try { ID = GetSegmentValue(AbbreviateEDIDocument, "N1", 4); } catch (Exception) { ID = ""; }
                            ADDRLINE1 = "";
                            try { CITY = GetSegmentValue(AbbreviateEDIDocument, "N4", 1); } catch (Exception) { CITY = ""; }
                            try { PROVSTATE = GetSegmentValue(AbbreviateEDIDocument, "N4", 2); } catch (Exception) { PROVSTATE = ""; }
                            try { POSTALCODE = GetSegmentValue(AbbreviateEDIDocument, "N4", 3); } catch (Exception) { POSTALCODE = ""; }
                            try { COUNTRY = GetSegmentValue(AbbreviateEDIDocument, "N4", 4); } catch (Exception) { COUNTRY = ""; }

                            DataSolutions.DataModels.Xml.ShipNotice.SHIPLOC Shiplevel_Shiploc_Type_ST = new DataSolutions.DataModels.Xml.ShipNotice.SHIPLOC
                            {
                                LOCTYPE = "ST",
                                NAME = NAME,
                                ID = ID,
                                ADDRLINE1 = "",
                                CITY = CITY,
                                PROVSTATE = PROVSTATE,
                                POSTALCODE = POSTALCODE,
                                COUNTRY = COUNTRY,
                            };
                            shipLevel.SHIPLOC = new List<DataSolutions.DataModels.Xml.ShipNotice.SHIPLOC> { Shiplevel_Shiploc_Type_ST }.ToArray();
                            asnDetails.Add(shipLevel);
                            //looping until next HL level
                            i++;
                            while (ediDocument.Segments[i].Name != "HL") { i++; }
                            i--;
                        }
                        #endregion

                        //Handling Order Level Segments
                        #region O Level
                        else if (ediDocument.Segments[i].Values[3] == "O")
                        {
                            currentHierarchyId++;
                            currentOrderLevelId = currentHierarchyId;

                            CartonSequence = 1;

                            MainLoop_O_Level += 1;

                            current_O_level_iteration++;

                            //Initiate new Order Level:
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL orderLevel = new DataSolutions.DataModels.Xml.ShipNotice.DETAIL();
                            orderLevel.HIERARCHY = new HIERARCHY { HIERID = currentHierarchyId.ToString(), PARENT = ediDocument.Segments[i].Values[2].ToString(), LEVELTYPE = "O" };
                            try { orderLevel.ORDERNUM = ediDocument.Segments[i + 1].Values[1]; } catch (Exception) { orderLevel.ORDERNUM = ""; }
                            try { orderLevel.PACKTYPE = GetSegmentValue(ediDocument, "TD1", 1); } catch (Exception) { orderLevel.PACKTYPE = ""; }
                            try { orderLevel.QUANTITY = Olevel_and_PQuantity[current_O_level_iteration].ToString(); } catch (Exception) { orderLevel.QUANTITY = ""; }
                            try { orderLevel.LADINGDESC = GetSegmentValue(ediDocument, "TD3", 1); } catch (Exception) { orderLevel.LADINGDESC = ""; }
                            try { orderLevel.CNTNRPRE = GetSegmentValue(ediDocument, "TD3", 2); } catch (Exception) { orderLevel.CNTNRPRE = ""; }
                            try { orderLevel.CNTNRNUM = GetSegmentValue(ediDocument, "TD3", 3); } catch (Exception) { orderLevel.CNTNRNUM = ""; }
                            try { orderLevel.SEALNUM = GetSegmentValue(ediDocument, "TD3", 9); } catch (Exception) { orderLevel.SEALNUM = ""; }
                            try { orderLevel.NETWGHT = ediDocument.Segments[i + 5].Values[7]; } catch (Exception) { orderLevel.NETWGHT = ""; }

                            //Create new EXTRA and map to order level
                            string CONTENT_netWeightUOM_Extra = "";
                            try { CONTENT_netWeightUOM_Extra = ediDocument.Segments[i + 5].Values[8]; } catch (Exception) { CONTENT_netWeightUOM_Extra = ""; };
                            DataSolutions.DataModels.Xml.ShipNotice.EXTRA netWeightUOM_Extra = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "999NWUOM", LABEL = "Net Weight UOM", CONTENT = CONTENT_netWeightUOM_Extra };
                            orderLevel.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { netWeightUOM_Extra }.ToArray();


                            //Mapping Party and add to Orderlevel
                            string NAME, ID, CITY, PROVSTATE, POSTALCODE, COUNTRY;
                            NAME = ID = CITY = PROVSTATE = POSTALCODE = COUNTRY = string.Empty;

                            try { NAME = ediDocument.Segments[i + 2].Values[2]; } catch (Exception) { NAME = ""; }
                            try { ID = ediDocument.Segments[i + 2].Values[4]; } catch (Exception) { ID = ""; }
                            if (ediDocument.Segments[i + 3].Name == "N4")
                            {
                                try { CITY = ediDocument.Segments[i + 3].Values[1]; } catch (Exception) { CITY = ""; }
                                try { PROVSTATE = ediDocument.Segments[i + 3].Values[2]; } catch (Exception) { PROVSTATE = ""; }
                                try { POSTALCODE = ediDocument.Segments[i + 3].Values[3]; } catch (Exception) { POSTALCODE = ""; }
                                try { COUNTRY = ediDocument.Segments[i + 3].Values[4]; } catch (Exception) { COUNTRY = ""; }
                            }
                            else
                            {
                                CITY = PROVSTATE = POSTALCODE = COUNTRY = string.Empty;
                            }


                            DataSolutions.DataModels.Xml.ShipNotice.PARTY Party_O_Level_98BY = new DataSolutions.DataModels.Xml.ShipNotice.PARTY
                            {
                                PARENT = "DETAIL",
                                CODE = "98BY",
                                LABEL = "Buyer Agent",
                                NAME = NAME,
                                ID = ID,
                                ADDRLINE1 = "",
                                CITY = CITY,
                                PROVSTATE = PROVSTATE,
                                POSTALCODE = POSTALCODE,
                                COUNTRY = COUNTRY
                            };

                            orderLevel.PARTY = new List<DataSolutions.DataModels.Xml.ShipNotice.PARTY> { Party_O_Level_98BY }.ToArray();


                            //Add newly created orderlevel to ASN Details List
                            asnDetails.Add(orderLevel);

                            i++;
                            while (ediDocument.Segments[i].Name != "HL") { i++; }
                            i--;
                        }
                        #endregion

                        #region P level
                        //Handling Pack Level Segments
                        else if (ediDocument.Segments[i].Values[3] == "P")
                        {
                            currentHierarchyId++;
                            PARENT_ID_FOR_I_LEVEL_EQUAL_P_LEVEL_HIREID = currentHierarchyId;
                            currentPackLevelId = currentOrderLevelId;


                            //Initiate new PACK Level:
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL packLevel = new DataSolutions.DataModels.Xml.ShipNotice.DETAIL();
                            packLevel.HIERARCHY = new HIERARCHY { HIERID = currentHierarchyId.ToString(), PARENT = currentOrderLevelId.ToString(), LEVELTYPE = "P" };

                            //getting the value from O to P level:
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL latestDetailWithLevelO = null;

                            for (int u = asnDetails.Count - 1; u >= 0; u--)
                            {
                                DataSolutions.DataModels.Xml.ShipNotice.DETAIL detail = asnDetails[u];

                                if (detail.HIERARCHY?.LEVELTYPE == "O")
                                {
                                    latestDetailWithLevelO = detail;
                                    break;
                                }
                            }
                            try { packLevel.ORDERNUM = latestDetailWithLevelO.ORDERNUM; } catch (Exception) { packLevel.ORDERNUM = ""; }


                            ///////////////////////////////////////////
                            try
                            {
                                for (int q = i; q < (i + 4); q++)
                                {
                                    if (ediDocument.Segments[q].Name == "MAN")
                                    {
                                        packLevel.CARTONID = ediDocument.Segments[q].Values[2];
                                    }
                                }

                            }
                            catch (Exception)
                            {
                                packLevel.CARTONID = "";
                            }


                            try
                            {
                                for (int v = i; v < (i + 4); v++)
                                {
                                    if (ediDocument.Segments[v].Name == "TD1")
                                    {
                                        packLevel.PACKTYPE = ediDocument.Segments[v].Values[1];

                                    }
                                }
                                packLevel.PACKTYPE = ediDocument.Segments[i + 1].Values[1];
                            }
                            catch (Exception)
                            {
                                packLevel.PACKTYPE = "";
                            }
                            //Calculate P level quantity by looping how many items in I level

                            //Looping Calculation
                            int ItemQuantitySum = 0;
                            int m = i;
                            if (ediDocument.Segments[m].Name == "HL")
                            {
                                if (ediDocument.Segments[m].Values[3] == "P")
                                {
                                    m++;
                                    while (ediDocument.Segments[m].Values[3] != "I" && ediDocument.Segments[m].Name != "HL") { m++; }
                                    while (ediDocument.Segments[m].Values[3] != "P" && ediDocument.Segments[m].Values[3] == "I")
                                    {
                                        ItemQuantitySum = ItemQuantitySum + Int32.Parse(ediDocument.Segments[m + 2].Values[2]);
                                        m += 4;
                                        try { if (ediDocument.Segments[m].Values[3] != "I") { break; } } catch (Exception) { break; }
                                    }
                                }
                            }
                            packLevel.QUANTITY = ItemQuantitySum.ToString();
                            try { packLevel.NETNETWGHT = ediDocument.Segments[i + 1].Values[7]; } catch (Exception) { packLevel.NETNETWGHT = ""; }
                            try { packLevel.VOLUME = ediDocument.Segments[i + 1].Values[9]; } catch (Exception) { packLevel.VOLUME = ""; }
                            try { packLevel.VOLUMEUOM = ediDocument.Segments[i + 1].Values[10]; } catch (Exception) { packLevel.VOLUMEUOM = ""; }


                            //BM REFNUM
                            string CONTENT_SHIPREFNUM_2I = "";
                            for (int k = i - 1; k < i + 3; k++)
                            {
                                if (ediDocument.Segments[k].Name == "MAN")
                                {
                                    try { CONTENT_SHIPREFNUM_2I = ediDocument.Segments[k].Values[3]; } catch (Exception) { CONTENT_SHIPREFNUM_2I = ""; }
                                }
                            }
                            SHIPREFNUM SHIPREFNUM_2I = new SHIPREFNUM { NUMTYPE = "2I", REFNUM = CONTENT_SHIPREFNUM_2I };

                            SHIPREFNUM SHIPREFNUM_3H = new SHIPREFNUM { NUMTYPE = "3H", REFNUM = CartonSequence.ToString() };
                            CartonSequence += 1;

                            packLevel.SHIPREFNUM = new List<SHIPREFNUM> { SHIPREFNUM_2I, SHIPREFNUM_3H }.ToArray();


                            //Extra 999CTN
                            string CONTENT_EXTRA_999CTN = "";
                            for (int k = i - 1; k < i + 3; k++)
                            {
                                if (ediDocument.Segments[k].Name == "MAN")
                                {
                                    try { CONTENT_EXTRA_999CTN = ediDocument.Segments[k].Values[6]; } catch (Exception) { CONTENT_EXTRA_999CTN = ""; }
                                }
                            }
                            DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999CTN = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "999CTN", LABEL = "Total Cartons", CONTENT = CONTENT_EXTRA_999CTN };

                            //Extra 999NWUOM
                            string CONTENT_EXTRA_999NWUOM = "";
                            try { CONTENT_EXTRA_999NWUOM = ediDocument.Segments[i + 1].Values[8]; } catch (Exception) { CONTENT_EXTRA_999NWUOM = ""; }
                            DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999NWUOM = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "999NWUOM", LABEL = "Net Weight UOM", CONTENT = CONTENT_EXTRA_999NWUOM };

                            packLevel.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { EXTRA_999CTN, EXTRA_999NWUOM }.ToArray();

                            //Add newly created PackLevel to ASN Details List
                            asnDetails.Add(packLevel);


                            //looping until next HL
                            i++;
                            while (ediDocument.Segments[i].Name != "HL") { i++; }
                            i--;
                        }
                        #endregion

                        //Handling Item Level Segments
                        #region I level
                        else if (ediDocument.Segments[i].Values[3] == "I")
                        {
                            currentHierarchyId++;
                            //currentItemLevelId = currentHierarchyId - 1;
                            currentItemLevelId = PARENT_ID_FOR_I_LEVEL_EQUAL_P_LEVEL_HIREID;

                            //Initiate new ITEM Level:
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL itemLevel = new DataSolutions.DataModels.Xml.ShipNotice.DETAIL();
                            itemLevel.HIERARCHY = new HIERARCHY { HIERID = currentHierarchyId.ToString(), PARENT = currentItemLevelId.ToString(), LEVELTYPE = "I" };

                            //ITEM tag for ITEM Level
                            string STYLE, ATTRCODE1, ATTRCODE2, ATTRCODE3, UPC;
                            STYLE = ATTRCODE1 = ATTRCODE2 = ATTRCODE3 = UPC = string.Empty;
                            try { STYLE = ediDocument.Segments[i + 1].Values[3]; } catch (Exception) { STYLE = ""; }
                            try { ATTRCODE1 = ediDocument.Segments[i + 1].Values[5]; } catch (Exception) { ATTRCODE1 = ""; }
                            try { ATTRCODE2 = ediDocument.Segments[i + 1].Values[7]; } catch (Exception) { ATTRCODE2 = ""; }
                            try { ATTRCODE3 = ediDocument.Segments[i + 1].Values[9]; } catch (Exception) { ATTRCODE3 = ""; }
                            try { UPC = ediDocument.Segments[i + 1].Values[15]; } catch (Exception) { UPC = ""; }

                            //Ordernum from O level to I level
                            DataSolutions.DataModels.Xml.ShipNotice.DETAIL latestDetailWithLevelO = null;

                            for (int u = asnDetails.Count - 1; u >= 0; u--)
                            {
                                DataSolutions.DataModels.Xml.ShipNotice.DETAIL detail = asnDetails[u];

                                if (detail.HIERARCHY?.LEVELTYPE == "O")
                                {
                                    latestDetailWithLevelO = detail;
                                    break;
                                }
                            }
                            try { itemLevel.ORDERNUM = latestDetailWithLevelO.ORDERNUM; } catch (Exception) { itemLevel.ORDERNUM = ""; }
                            /////////////////////////////////////////

                            itemLevel.ITEM = new ITEM
                            {
                                STYLE = STYLE,
                                ATTRCODE1 = ATTRCODE1,
                                ATTRCODE2 = ATTRCODE2,
                                ATTRCODE3 = ATTRCODE3,
                                UPC = UPC
                            };

                            //Main attributes
                            try { itemLevel.QUANTITY = ediDocument.Segments[i + 2].Values[2]; } catch (Exception) { itemLevel.QUANTITY = ""; };
                            try { itemLevel.QTYUOM = ediDocument.Segments[i + 2].Values[3]; } catch (Exception) { itemLevel.QTYUOM = ""; };


                            //Extra attributes

                            //Extra 999CTN - LIN11
                            string CONTENT_EXTRA_235CG = "";
                            try { CONTENT_EXTRA_235CG = ediDocument.Segments[i + 1].Values[11]; } catch (Exception) { CONTENT_EXTRA_235CG = ""; };
                            DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_235CG = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "235CG", LABEL = "Pre-Pack Number", CONTENT = CONTENT_EXTRA_235CG };

                            //Extra 235SE - REF01
                            string CONTENT_EXTRA_235SE = "";
                            try { CONTENT_EXTRA_235SE = ediDocument.Segments[i + 3].Values[2]; } catch (Exception) { CONTENT_EXTRA_235SE = ""; };
                            DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_235SE = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DETAIL", CODE = "235SE", LABEL = "Season Code", CONTENT = CONTENT_EXTRA_235SE };

                            itemLevel.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { EXTRA_235CG, EXTRA_235SE }.ToArray();

                            //Add newly created ItemLevel to ASN Details List
                            asnDetails.Add(itemLevel);

                            i++;
                            while (ediDocument.Segments[i].Name != "HL")
                            {
                                i++;
                                if (i == ediDocument.Segments.Count()) { break; }
                            }
                            i--;

                        }
                        #endregion

                    }
                    else
                    {
                        Logger.Info("Reaching to the end of the input EDI file", BuyerShortCode, CorrelationId);
                    }

                }

                translatedDoc856.DETAIL = asnDetails.ToArray();

                #region MISC Extras
                //DOC856 EXTRAS

                //EXTRA 999ISASID - ISA05+ISA06 - Interchange Sender ID
                string CONTENT_EXTRA_999ISASID = "";
                try { CONTENT_EXTRA_999ISASID = GetSegmentValue(ediDocument, "ISA", 5) + "/" + GetSegmentValue(ediDocument, "ISA", 6); } catch (Exception) { CONTENT_EXTRA_999ISASID = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ISASID = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ISASID", LABEL = "Interchange Sender ID", CONTENT = CONTENT_EXTRA_999ISASID };

                //EXTRA 999ISARID - ISA07 + ISA08 - Interchange Receiver ID
                string CONTENT_EXTRA_999ISARID = "";
                try { CONTENT_EXTRA_999ISARID = GetSegmentValue(ediDocument, "ISA", 7) + "/" + GetSegmentValue(ediDocument, "ISA", 8); } catch (Exception) { CONTENT_EXTRA_999ISARID = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ISARID = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ISARID", LABEL = "Interchange Receiver ID", CONTENT = CONTENT_EXTRA_999ISARID };

                //EXTRA 999ISADT - ISA09 - EDI Interchange Date
                string CONTENT_EXTRA_999ISADT = "";
                try { CONTENT_EXTRA_999ISADT = GetSegmentValue(ediDocument, "ISA", 9); } catch (Exception) { CONTENT_EXTRA_999ISADT = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ISADT = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ISADT", LABEL = "EDI Interchange Date", CONTENT = CONTENT_EXTRA_999ISADT };

                //EXTRA 999ISATM - ISA10 - EDI Interchange Time
                string CONTENT_EXTRA_999ISATM = "";
                try { CONTENT_EXTRA_999ISATM = GetSegmentValue(ediDocument, "ISA", 10); } catch (Exception) { CONTENT_EXTRA_999ISATM = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ISATM = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ISATM", LABEL = "EDI Interchange Time", CONTENT = CONTENT_EXTRA_999ISATM };

                //EXTRA 999ISA - ISA13 - Interchange Control Number
                string CONTENT_EXTRA_999ISA = "";
                try { CONTENT_EXTRA_999ISA = GetSegmentValue(ediDocument, "ISA", 11); } catch (Exception) { CONTENT_EXTRA_999ISA = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ISA = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ISA", LABEL = "Interchange Control Number", CONTENT = CONTENT_EXTRA_999ISA };

                //EXTRA GS06 - 999GS - Group Control Number
                string CONTENT_EXTRA_999GS = "";
                try { CONTENT_EXTRA_999GS = GetSegmentValue(ediDocument, "GS", 6); } catch (Exception) { CONTENT_EXTRA_999GS = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999GS = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999GS", LABEL = "Group Control Number", CONTENT = CONTENT_EXTRA_999GS };

                //EXTRA 999ST - ST02 - Transaction Set Control Number
                string CONTENT_EXTRA_999ST = "";
                try { CONTENT_EXTRA_999ST = GetSegmentValue(ediDocument, "ST", 2); } catch (Exception) { CONTENT_EXTRA_999ST = ""; };
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999ST = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999ST", LABEL = "Transaction Set Control Number", CONTENT = CONTENT_EXTRA_999ST };

                //EXTRA - Document Nature
                DataSolutions.DataModels.Xml.ShipNotice.EXTRA EXTRA_999DOCNAT = new DataSolutions.DataModels.Xml.ShipNotice.EXTRA { PARENT = "DOC856", CODE = "999DOCNAT", LABEL = "Document Nature", CONTENT = "ISHFRBUY" };

                translatedDoc856.EXTRA = new List<DataSolutions.DataModels.Xml.ShipNotice.EXTRA> { EXTRA_999ISASID, EXTRA_999ISARID, EXTRA_999ISADT, EXTRA_999ISATM, EXTRA_999ISA, EXTRA_999GS, EXTRA_999ST, EXTRA_999DOCNAT }.ToArray();
                #endregion

            }
            #endregion
            catch (Exception ex)
            {
                //getting full path of failed edi file for email
                var inputEDIFiles = (new DirectoryInfo(_inputFolder)).GetFiles($"*.{_inputFileExtension}").ToList();
                string edifilenameWithExtension = Path.GetFullPath(inputEDIFiles[0].FullName);


                //If ASN translation fails, an email alert would be sent.
                Logger.Info(ediFileName + ".edi" + " file cannot be translated", BuyerShortCode, DocumentCode, CorrelationId);
                Logger.Error("Error during generating XML, find exception for more details... " + "The input EDI file will be moved to failed folder and not be further translated to DOC856", ex, BuyerShortCode, DocumentCode, CorrelationId, ediFileName, 0);

                translatedDoc856 = null;

                #region email success = 1 -> REPORTED folder, = 0 --> XFAILED folder
                //send email successfully
                if (EmailAlerts(GetSegmentValue(ediDocument, "BSN", 2), ediFileName, edifilenameWithExtension))
                {
                    FileMovementRetry FileMovementRetryFunctions = new FileMovementRetry(BuyerShortCode, _fileRetryMovement_Connection_String);
                    FileMovementRetryFunctions.MoveToDestination(Path.Combine(_inputFolder, ediFileName + ".edi"), Path.Combine(_ReportFolder, ediFileName + ".edi"), "EDI856", _workingFolder);
                }
                //send email failed
                else
                {
                    FileMovementRetry FileMovementRetryFunctions = new FileMovementRetry(BuyerShortCode, _fileRetryMovement_Connection_String);
                    FileMovementRetryFunctions.MoveToDestination(Path.Combine(_inputFolder, ediFileName + ".edi"), Path.Combine(_xfailedFolder, ediFileName + ".edi"), "EDI856", _workingFolder);
                }
                #endregion
            }
            Logger.Info("Leaving CartersOSVVendorInboundEDI856MappingProcess.GenerateXML856()", BuyerShortCode, CorrelationId);
            return translatedDoc856;
        }
        private string GetSegmentValue(EdiDocument ediDocument, string segmentName, int position)
        {
            string value = "";
            foreach (var es in ediDocument.Segments)
            {
                if (es.Name == segmentName)
                {
                    if (position <= (es.Values.Count() - 1))
                    {
                        value = es.Values[position].Trim();
                        break;
                    }
                    else
                    {
                        return value;
                    }
                }
            }
            return value;
        }

        public bool EmailAlerts(string asnNumber, string filename, string FullPathForAttachment)
        {
            try
            {
                string subject = "TLT ALERT: Carter’s OSV Vendor Inbound EDI856 Warning - Translation Failure";
                string receiver = ConfigurationManager.AppSettings["CRR_AlertRecipients"];
                string ccReceiver = ConfigurationManager.AppSettings["CRR_AlertCopyToRecipients"];

                body = "Dear PS Team,<br><br>"
                      + "The EDI856 file could not be translated to XML856 format successfully.<br>"
                      + "<table border='1' style='border-collapse:collapse;'><tr><th>ASN Number</th></tr>"
                      + $"<tr><td>{asnNumber}</td></tr></table><br><br>"
                      + $"EDI856 Filename:  {filename} <br><br>"
                      + "Please check and follow up with ShenZhen Development Team for further action.<br><br>"
                      + $"Regards,<br>Data Solutions Team<br>TradeLinkTechnologies Ltd.";

                sendEmail.Send(receiver, ccReceiver, subject, body, FullPathForAttachment, true);
                Logger.Info("Email Alert is sent", BuyerShortCode, DocumentCode, CorrelationId);
                return true;
            }
            catch (Exception ex)
            {
                Logger.Info("Email Alert can not be sent", BuyerShortCode, DocumentCode, CorrelationId);
                return false;
            }
        }
    }
}

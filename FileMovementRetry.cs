using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using System.Runtime.InteropServices;
using System.IO;

namespace DataSolutions.Commons.FileMovementRetry
{
    public class FileMovementRetry
    {
        private readonly string BUYER_SHORT_CODE;
        private readonly string CONNECTION_STRING;

        public FileMovementRetry(string BuyerShortCode, string connString)
        {
            BUYER_SHORT_CODE = BuyerShortCode;
            CONNECTION_STRING = connString;
        }

        public void insertToDB(string BuyerCode, string DocType, string SourceFile, string DestinationPath, string Action, DateTime currentDateTime, string ConnectionString)
        {
            int rowsAdded = 0;
            using (var connection = new SqlConnection(ConnectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = @"INSERT INTO FileTransferRecord (BuyerCode,DocType,SourceFile,DestinationPath,Action,CreatedTime) VALUES (@BuyerCode,@DocType,@SourceFile,@DestinationPath,@Action,@CreatedTime)";
                    command.Parameters.AddWithValue("@BuyerCode", BuyerCode);
                    command.Parameters.AddWithValue("@DocType", DocType);
                    command.Parameters.AddWithValue("@SourceFile", SourceFile);
                    command.Parameters.AddWithValue("@DestinationPath", DestinationPath);
                    command.Parameters.AddWithValue("@Action", Action);
                    command.Parameters.AddWithValue("@CreatedTime", currentDateTime);
                    connection.Open();
                    rowsAdded = command.ExecuteNonQuery();

                }
                connection.Close();
            }
        }

        public bool CheckIfRecordExists(string BuyerCode, string DocType, string SourceFile, string DestinationPath, string Action, DateTime createdTime, string ConnectionString)
        {
            SqlConnection conn = new SqlConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
                conn.Open();

            SqlCommand check_Record = new SqlCommand("SELECT * FROM FileTransferRecord WHERE BuyerCode = @BuyerCode AND DocType = @DocType AND SourceFile = @SourceFile AND DestinationPath = @DestinationPath AND Action = @Action", conn);
            check_Record.Parameters.AddWithValue("@BuyerCode", BuyerCode);
            check_Record.Parameters.AddWithValue("@DocType", DocType);
            check_Record.Parameters.AddWithValue("@SourceFile", SourceFile);
            check_Record.Parameters.AddWithValue("@DestinationPath", DestinationPath);
            check_Record.Parameters.AddWithValue("@Action", Action);

            SqlDataReader reader = check_Record.ExecuteReader();
            if (reader.HasRows)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void deleteRecord(string BuyerCode, string DocType, string SourceFile, string DestinationPath, string Action, DateTime createdTime, string ConnectionString)
        {
            try
            {
                using (var sc = new SqlConnection(ConnectionString))
                using (var cmd = sc.CreateCommand())
                {
                    sc.Open();
                    cmd.CommandText = "DELETE FROM FileTransferRecord WHERE BuyerCode = @BuyerCode AND DocType = @DocType AND SourceFile = @SourceFile AND DestinationPath = @DestinationPath AND Action = @Action";
                    cmd.Parameters.AddWithValue("@BuyerCode", BuyerCode);
                    cmd.Parameters.AddWithValue("@DocType", DocType);
                    cmd.Parameters.AddWithValue("@SourceFile", SourceFile);
                    cmd.Parameters.AddWithValue("@DestinationPath", DestinationPath);
                    cmd.Parameters.AddWithValue("@Action", Action);

                    cmd.ExecuteNonQuery();
                }
            }
            catch (SystemException ex)
            {
                Console.WriteLine(string.Format("An error occurred: {0}", ex.Message));
            }
        }

        public bool MoveToArchiveOutputFile(string path, string archiveFolderPath, string FileType, string TmpFilePath)
        {
            string returnPath = "";
            DateTime current = DateTime.Now;

            string year = current.ToString("yyyy");
            string month = current.ToString("MM");
            string day = current.ToString("dd");

            string fileName = Path.GetFileName(path);
            try
            {
                DirectoryInfo di = Directory.CreateDirectory(archiveFolderPath + "\\" + year + "\\" + month + "\\" + day + "\\");
                returnPath = di.FullName;
                string fileArchivePath = Path.Combine(di.FullName, Path.GetFileName(path));

                if (File.Exists(fileArchivePath))
                {
                    File.Delete(fileArchivePath);
                    File.Move(path, fileArchivePath);
                }
                else
                {
                    File.Move(path, fileArchivePath);
                }
                return true;
            }
            catch (Exception)
            {
                string current_File_Path_Tmp = TmpFilePath + "\\" + fileName;
                File.Move(path, current_File_Path_Tmp);

                string directoryPath = Path.Combine(archiveFolderPath, year, month, day);
                string fileArchivePath = Path.Combine(directoryPath, Path.GetFileName(path));
 
                insertToDB(BUYER_SHORT_CODE, FileType, current_File_Path_Tmp, fileArchivePath, "MOVE", DateTime.Now, CONNECTION_STRING);
                return false;
            }

        }

        public bool CopyToDestination(string path, string destinationPath, string FileType, string TmpFilePath)
        {
            string fileDestinationPath = destinationPath;
            string fileName = Path.GetFileName(path);

            try
            {
                if (File.Exists(fileDestinationPath))
                {
                    File.Delete(fileDestinationPath);
                    File.Copy(path, fileDestinationPath);
                }
                else
                {
                    File.Copy(path, fileDestinationPath);
                }
                return true;
            }
            catch (Exception)
            {
                insertToDB(BUYER_SHORT_CODE, FileType, path, fileDestinationPath, "COPY", DateTime.Now, CONNECTION_STRING);
                return false;
            }
        }

        public bool MoveToDestination(string path, string destinationPath, string FileType, string TmpFilePath)
        {
            string fileDestinationPath = destinationPath;
            string fileName = Path.GetFileName(path);

            try
            {
                // Create the directory structure if it doesn't exist
                Directory.CreateDirectory(Path.GetDirectoryName(fileDestinationPath));

                if (File.Exists(fileDestinationPath))
                {
                    File.Delete(fileDestinationPath);
                    File.Move(path, fileDestinationPath);
                }
                else
                {
                    File.Move(path, fileDestinationPath);
                }
                return true;
            }
            catch (Exception)
            {
                string current_File_Path_Tmp = TmpFilePath + "\\" + fileName;
                File.Move(path, current_File_Path_Tmp);
                insertToDB(BUYER_SHORT_CODE, FileType, current_File_Path_Tmp, fileDestinationPath, "MOVE", DateTime.Now, CONNECTION_STRING);
                return false;

            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AddUpdateRecipientListToSchoolScheduleDetails
{
    public class WriteToLog
    {
        public void WriteToLogFile(string logTxt)
        {
            string filename = ConfigurationManager.AppSettings["logFilePath"];
            StreamWriter logFile;
            try
            {
                if (!File.Exists(filename))
                {
                    logFile = new StreamWriter(filename);
                }
                else
                {
                    logFile = File.AppendText(filename);
                }

                logFile.WriteLine(DateTime.Now + ": " + logTxt.ToString());
                logFile.Close();
            }
            catch (Exception ex)
            {

            }
        }
    }
}

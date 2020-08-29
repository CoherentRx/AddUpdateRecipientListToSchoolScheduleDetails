using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using CRXServices.BigQuery;
using CRXServices.EmailServices;
using AddUpdateRecipientListToSchoolScheduleDetails.Model;
using System.Data.SqlClient;
using Dapper;
using System.Data;

namespace AddUpdateRecipientListToSchoolScheduleDetails
{
    class Program
    {
        private static string _bqProjectID = ConfigurationManager.AppSettings["bqProjectID"];
        private static string _bqCredentialsPath = ConfigurationManager.AppSettings["bqCredentialsPath"];
        private static string _connectionString = ConfigurationManager.ConnectionStrings["CoherentRX_Production"].ToString();


        static void Main(string[] args)
        {
            WriteToLog LogObj = new WriteToLog();

            LogObj.WriteToLogFile("AddUpdateRecipientListToSchoolScheduleDetails process Starts: " + DateTime.Now.ToLongDateString() + " " + DateTime.Now.ToLongTimeString());

            //Get data from bigQuery
            var bqService = new BigQueryService(_bqProjectID, _bqCredentialsPath);
            var phoneListForSchool = bqService.GetData<MasterRoster>(procedureName: "call testdataprepr.DS_Dashboard.sp_GetConcatePhoneListBasedSchoolID()");



            var schoolID = string.Empty;
            foreach(var phonelist in phoneListForSchool)
            {
                schoolID = phonelist.SchoolID;

                //Update SchoolScheduleDetails table based on the SchoolID

                UpdateSchoolScheduleDetailBasedSchoolID(schoolID, phonelist.PhoneList);
                LogObj.WriteToLogFile("------------------------------");
            }

            LogObj.WriteToLogFile("AddUpdateRecipientListToSchoolScheduleDetails process Ended: " + DateTime.Now.ToLongDateString() + " " + DateTime.Now.ToLongTimeString());
            LogObj.WriteToLogFile("-----------------------------------------------------------------------------------------------------------");

        }

        static void UpdateSchoolScheduleDetailBasedSchoolID(string SchoolID, string RecipientList)
        {
            WriteToLog LogObj = new WriteToLog();

            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    DynamicParameters _params = new DynamicParameters();
                    _params.Add("@SchoolID", SchoolID);
                    _params.Add("@RecipientList", RecipientList);
                    _params.Add("@schoolExist", DbType.Int32, direction: ParameterDirection.Output);
                    var result = connection.Execute("UpdateSchoolScheduleDetailBasedSchoolID", _params, null, null, CommandType.StoredProcedure);
                    var retVal = _params.Get<int>("schoolExist");

                    //connection.Execute("UpdateSchoolScheduleDetailBasedSchoolID",
                    //        param: parms, commandType: CommandType.StoredProcedure);

                    if(retVal == 1)
                    {
                        LogObj.WriteToLogFile("Update the RecipientList for SchoolID : " + SchoolID);

                    }
                    else
                    {
                        LogObj.WriteToLogFile("Entry does not exist for SchoolID : " + SchoolID);

                    }
                }
                catch(Exception ex)
                {
                    LogObj.WriteToLogFile("Update the RecipientList for SchoolID Failed : " + SchoolID);

                }
            }


        }
    }
}

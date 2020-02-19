using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DataBaseCode
{
    public class MedicineWithChangedValues
    {
        public string MedicineId { get; set; }
        public int MedicineNo { get; set; }
        public string OldType { get; set; }
        public string NewType { get; set; }

        public string OldGroup { get; set; }
        public string NewGroup { get; set; }

        public string OldUnitOfMeasure { get; set; }
        public string NewUnitOfMeasure { get; set; }
    }

    public class MedicineGroup
    {
        public string Name { get; set; }
        public string Id { get; set; }
    }

    public class PatientWithChangedBloodGroup
    {
        public int PatientId { get; set; }
        public string OldBloodGroup { get; set; }
        public string NewBloodGroup { get; set; }
    }

    public class PatientWithChangedDepartment
    {
        public int PatientId { get; set; }
        public string OldDepartment { get; set; }
        public string NewDepartment { get; set; }
    }

    // Note- Everything is currently being added to locationid = 1. The userId will be that of an admin, so that we know that these are records that have initially been added by our developers and not while in use by doctors.
    public class DataPorting
    {
        #region Connection related properties
        private string connectionStringForAccessDB = @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=###";
        private string connectionStringForMySql = @"server=localhost;user =root; database = ###; port=3306; password =###";
        private DataTable dataTable = null;
        #endregion

        #region Common properties for all table porting
        private int locationId = 1;
        public string adminUserId = Guid.NewGuid().ToString(); // This will be used as id for admin user and will be used in all tables as UserId initially to distinguish that the records are ported ones. 
        private string defaultCreatedDate = new DateTime().AddYears(1900).AddDays(2).ToString("yyyy-MM-dd hh:mm:ss");//  "1901-01-03 00:00:00";
        #endregion

        #region Properties used for storing codes/ids of one table for porting related tables 
        Dictionary<string, string> Roles;                        // this is needed for usersinroles
        Dictionary<string, string> Users;                        // this is needed for usersinroles
        Dictionary<string, MedicineGroup> MedicineGroups;        // this is needed for medicines + dept_group_priorities
        Dictionary<string, string> Departments;                  // this is needed for all tables where departments are used + dept_group_priorities
        Dictionary<string, string> ComplicationCategories;       // this is needed for ComplicationsMaster table 
        Dictionary<string, string> ComplicationCodes;           // this is needed for Complications table 
        Dictionary<string, string> SymptomCategories;            // this is needed for SymptomMaster table 
        Dictionary<string, string> SymptomCodes;                // this is needed for Symptom table 
        Dictionary<string, string> SignCategories;              // this is needed for SignMaster table 
        Dictionary<string, string> SignCodes;                // this is needed for Sign table 
        Dictionary<string, string> TestCategories;              // this is needed for InvestigationMaster table 
        Dictionary<string, string> TestCodes;                   // this is needed for dept_test_priorities and catg_test_priorities and investigations table 
        Dictionary<string, string> ShortcutDataEntryTypes;       // this is needed for shortcutdataentrytypes
        Dictionary<string, string> ShortcutKeys;                 // this is needed for shortcuts
        Dictionary<string, string> FormFormats;                  // this is needed for forms
        Dictionary<int, string> MedicineNo_MedicineId_Mapping; // this is needed for prescriptions
        List<int> PatientIds;                                    // this is needed for prescriptions
        #endregion

        #region Lists of records changed (requiring verification) or not inserted due to exceptions in data
        public List<PatientWithChangedBloodGroup> patientsWithBlankedBloodGroups;       // to get a list of all the patientids whose bloodgroups did not belong to any valid blood group and were made NA
        public List<PatientWithChangedDepartment> patientsWithGeneralDepartments;       // to get a list of all the patientids whose departments did not belong to any valid department and were assigned to GENERAL department
        public Dictionary<string, string> PatientRecordsWithExceptionsDict;
        public Dictionary<string, string> MedicineRecordsWithExceptionsDict;
        public Dictionary<string, string> PrescriptionRecordsWithExceptionsDict { get; set; }
        public Dictionary<string, string> PrescriptionCommentRecordsWithExceptionsDict { get; set; }
        public Dictionary<string, string> EventRecordsWithExceptionsDict { get; set; }
        public Dictionary<string, string> IllnessRecordsWithExceptionsDict { get; set; }
        public List<string> NewlyAddedComplicationCategories { get; set; }
        public Dictionary<string, string> ComplicationMasterRecordsWithExceptionsDict { get; set; }
        public List<string> NewlyAddedSymptomCategories { get; set; }
        public Dictionary<string, string> SymptomMasterRecordsWithExceptionsDict { get; set; }
        public List<string> NewlyAddedSignCategories { get; set; }
        public Dictionary<string, string> SignMasterRecordsWithExceptionsDict { get; set; }
        public List<string> NewlyAddedTestCategories { get; set; }
        public Dictionary<string, string> InvestigationMasterRecordsWithExceptionsDict { get; set; }
        public List<MedicineWithChangedValues> MedicinesWithChangedValues { get; set; }
        public Dictionary<string, string> InvalidDeptTestPriorities { get; set; }
        public Dictionary<string, string> InvalidCatgTestPriorities { get; set; }
        public Dictionary<string, string> InvalidComplications { get; set; }
        public Dictionary<string, string> InvalidSymptoms { get; set; }
        public Dictionary<string, string> InvalidSigns { get; set; }
        public Dictionary<string, string> InvalidInvestigations { get; set; }
        public Dictionary<string, string> InvalidForms { get; set; }
        public List<string> DuplicatePrescriptionsSetWithDeletedDate { get; set; }
        public List<string> DuplicateInvestigationsSetWithDeletedDate { get; set; }

        #endregion

        public DataPorting()
        {
            #region Initialize the roles
            Roles = new Dictionary<string, string>();
            Roles["Admin"] = Guid.NewGuid().ToString();
            Roles["Viewer"] = Guid.NewGuid().ToString();
            Roles["Editor"] = Guid.NewGuid().ToString();
            Roles["AppMaster"] = Guid.NewGuid().ToString();
            #endregion

            #region Initialize the users 
            Users = new Dictionary<string, string>();
            Users["Leadows"] = adminUserId;
            Users["xxx"] = Guid.NewGuid().ToString();
            Users["xx"] = Guid.NewGuid().ToString();
            #endregion

            #region Initialize the groups dictionary with key = unique groupnames in med_catg column of medicines table in AccessDB, value= actual group

            MedicineGroups = new Dictionary<string, MedicineGroup>();
            MedicineGroups.Add("AKT", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "AKT"
            });

            MedicineGroups.Add("ANTIBIOTIC", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "ANTIBIOTIC"
            });

            MedicineGroups.Add("CNS", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "CNS"
            });

            var cvsGroup = new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "CVS"
            };
            MedicineGroups.Add("CARDIAC", cvsGroup);
            MedicineGroups.Add("CVS", cvsGroup);

            var diabGroup = new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "DIAB"
            };
            MedicineGroups.Add("DIAB", diabGroup);
            MedicineGroups.Add("GDIABEN", diabGroup);

            var genGroup = new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "GEN"
            };
            MedicineGroups.Add("GE", genGroup);
            MedicineGroups.Add("GEN", genGroup);
            MedicineGroups.Add("GENGEN", genGroup);
            MedicineGroups.Add("GENN", genGroup);

            MedicineGroups.Add("HD", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "HD"
            });

            var htnGroup = new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "HTN"
            };
            MedicineGroups.Add("HBP", htnGroup);
            MedicineGroups.Add("HTN", htnGroup);

            var nephroGroup = new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "NEPHRO"
            };
            MedicineGroups.Add("NEPCVSHRO", nephroGroup);
            MedicineGroups.Add("NEPHRO", nephroGroup);

            MedicineGroups.Add("TB", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "TB"
            });

            MedicineGroups.Add("TRANSPLANT", new MedicineGroup()
            {
                Id = Guid.NewGuid().ToString(),
                Name = "TRANSPLANT"
            });
            #endregion

            #region Initialize Departments
            Departments = new Dictionary<string, string>();
            Departments.Add("DIAB", Guid.NewGuid().ToString());
            Departments.Add("TRANSPLANT", Guid.NewGuid().ToString());
            Departments.Add("NEPHRO", Guid.NewGuid().ToString());
            Departments.Add("GENERAL", Guid.NewGuid().ToString());
            #endregion

            #region Initialize ShortcutDataEntryTypes
            ShortcutDataEntryTypes = new Dictionary<string, string>();
            ShortcutDataEntryTypes.Add("Patient Form", Guid.NewGuid().ToString());
            ShortcutDataEntryTypes.Add("Medicine Dosage", Guid.NewGuid().ToString());
            ShortcutDataEntryTypes.Add("Test", Guid.NewGuid().ToString());
            ShortcutDataEntryTypes.Add("Illness", Guid.NewGuid().ToString());
            ShortcutDataEntryTypes.Add("Events", Guid.NewGuid().ToString());
            #endregion

            #region Initialize Shortcut Keys
            ShortcutKeys = new Dictionary<string, string>();

            ShortcutKeys.Add("F1", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F2", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F3", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F4", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F5", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F6", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F7", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F8", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F9", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F10", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F11", Guid.NewGuid().ToString());
            ShortcutKeys.Add("F12", Guid.NewGuid().ToString());
            #endregion

            #region Initialize Complication Categories
            ComplicationCategories = new Dictionary<string, string>();
            ComplicationCategories["GEN"] = Guid.NewGuid().ToString();
            ComplicationCategories["TRANSPLANT"] = Guid.NewGuid().ToString();
            ComplicationCategories["DIAB"] = Guid.NewGuid().ToString();
            ComplicationCategories["NEPHRO"] = Guid.NewGuid().ToString();
            ComplicationCategories["URO"] = Guid.NewGuid().ToString();

            #endregion

            #region Initialize Symptom Categories
            SymptomCategories = new Dictionary<string, string>();
            SymptomCategories["GEN"] = Guid.NewGuid().ToString();
            SymptomCategories["CVS"] = Guid.NewGuid().ToString();
            SymptomCategories["GIT"] = Guid.NewGuid().ToString();
            SymptomCategories["CNS"] = Guid.NewGuid().ToString();
            SymptomCategories["NEPHRO"] = Guid.NewGuid().ToString();
            SymptomCategories["R/S"] = Guid.NewGuid().ToString();
            SymptomCategories["DIAB"] = Guid.NewGuid().ToString();
            #endregion

            #region Initialize Sign Categories
            SignCategories = new Dictionary<string, string>();
            SignCategories["GEN EXAM"] = Guid.NewGuid().ToString();
            SignCategories["CVS"] = Guid.NewGuid().ToString();
            SignCategories["A/S"] = Guid.NewGuid().ToString();
            SignCategories["CNS"] = Guid.NewGuid().ToString();
            SignCategories["R/S"] = Guid.NewGuid().ToString();
            SignCategories["DIAB"] = Guid.NewGuid().ToString();
            #endregion

            #region Initialize Test Categories
            TestCategories = new Dictionary<string, string>();
            TestCategories["GENERAL"] = Guid.NewGuid().ToString();
            TestCategories["HEART"] = Guid.NewGuid().ToString();
            TestCategories["LIVER"] = Guid.NewGuid().ToString();
            TestCategories["NEPHRO"] = Guid.NewGuid().ToString();
            TestCategories["DIAB"] = Guid.NewGuid().ToString();
            #endregion

            patientsWithBlankedBloodGroups = new List<PatientWithChangedBloodGroup>();
            patientsWithGeneralDepartments = new List<PatientWithChangedDepartment>();
            PatientRecordsWithExceptionsDict = new Dictionary<string, string>();
            MedicineRecordsWithExceptionsDict = new Dictionary<string, string>();
            FormFormats = new Dictionary<string, string>();
            MedicineNo_MedicineId_Mapping = new Dictionary<int, string>();
            PatientIds = new List<int>();
            PrescriptionRecordsWithExceptionsDict = new Dictionary<string, string>();
            PrescriptionCommentRecordsWithExceptionsDict = new Dictionary<string, string>();
            EventRecordsWithExceptionsDict = new Dictionary<string, string>();
            IllnessRecordsWithExceptionsDict = new Dictionary<string, string>();
            NewlyAddedComplicationCategories = new List<string>();
            ComplicationMasterRecordsWithExceptionsDict = new Dictionary<string, string>();
            NewlyAddedSymptomCategories = new List<string>();
            SymptomMasterRecordsWithExceptionsDict = new Dictionary<string, string>();
            NewlyAddedSignCategories = new List<string>();
            SignMasterRecordsWithExceptionsDict = new Dictionary<string, string>();
            NewlyAddedTestCategories = new List<string>();
            InvestigationMasterRecordsWithExceptionsDict = new Dictionary<string, string>();
            TestCodes = new Dictionary<string, string>();
            InvalidDeptTestPriorities = new Dictionary<string, string>();
            InvalidCatgTestPriorities = new Dictionary<string, string>();
            ComplicationCodes = new Dictionary<string, string>();
            SymptomCodes = new Dictionary<string, string>();
            SignCodes = new Dictionary<string, string>();
            InvalidComplications = new Dictionary<string, string>();
            InvalidSymptoms = new Dictionary<string, string>();
            InvalidSigns = new Dictionary<string, string>();
            InvalidInvestigations = new Dictionary<string, string>();
            InvalidForms = new Dictionary<string, string>();
            DuplicatePrescriptionsSetWithDeletedDate = new List<string>();
            DuplicateInvestigationsSetWithDeletedDate = new List<string>();
        }

        #region Helper Methods
        // Helper method for portshortcuts
        private async Task<string> GetKeyId(int key)
        {
            // TODO - Find a better way to check which key 
            string keyId = string.Empty;
            if (key == 112) // F1
                keyId = ShortcutKeys["F1"];
            else if (key == 113) // F2
                keyId = ShortcutKeys["F2"];
            else if (key == 114) // F3
                keyId = ShortcutKeys["F3"];
            else if (key == 115) // F4
                keyId = ShortcutKeys["F4"];
            else if (key == 116) // F5
                keyId = ShortcutKeys["F5"];
            else if (key == 117) // F6
                keyId = ShortcutKeys["F6"];
            else if (key == 118) // F7
                keyId = ShortcutKeys["F7"];
            else if (key == 119) // F8
                keyId = ShortcutKeys["F8"];
            else if (key == 120) // F9
                keyId = ShortcutKeys["F9"];
            else if (key == 121) // F10
                keyId = ShortcutKeys["F10"];
            else if (key == 122) // F11
                keyId = ShortcutKeys["F11"];
            else if (key == 123) // F12
                keyId = ShortcutKeys["F12"];
            else // There is a new shortcut key - add it's converted key to the shortcutkeys table and to the dictionary, and set to keyid 
            {
                KeysConverter kc = new KeysConverter();
                string keyChar = kc.ConvertToString(key);

                ShortcutKeys.Add(keyChar, Guid.NewGuid().ToString());
                keyId = ShortcutKeys[keyChar];

                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into shortcutkeys(Id,KeyName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{keyId}','{keyChar}',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }

            return keyId;
        }

        // Helper method for patients
        private string GetValidDepartment(string oldDepartmentName, int patientId)
        {
            string newDepartmentId = string.Empty;
            if (Departments.Keys.Contains(oldDepartmentName.ToUpper()))
                newDepartmentId = Departments[oldDepartmentName.ToUpper()];
            else
            {
                newDepartmentId = Departments["GENERAL"];
                patientsWithGeneralDepartments.Add(new PatientWithChangedDepartment()
                {
                    PatientId = patientId,
                    OldDepartment = oldDepartmentName,
                    NewDepartment = "GENERAL"
                });
            }
            return newDepartmentId;
        }

        // Helper method for patients
        private string GetValidBloodGroup(string oldBloodGroup, int patientId)
        {
            string newBloodGroup = string.Empty;
            if (string.IsNullOrWhiteSpace(oldBloodGroup))
            {
                newBloodGroup = "NA";
                patientsWithBlankedBloodGroups.Add(new PatientWithChangedBloodGroup()
                {
                    PatientId = patientId,
                    OldBloodGroup = oldBloodGroup,
                    NewBloodGroup = newBloodGroup
                });
                return newBloodGroup;
            }
            else
                oldBloodGroup = oldBloodGroup.Trim();

            if (oldBloodGroup.Contains("A-") || oldBloodGroup.Contains("A -") || oldBloodGroup.Contains("A NEG"))
                newBloodGroup = "A-";
            else if (oldBloodGroup.Contains("B NEG") || oldBloodGroup.Contains("B-") || oldBloodGroup.Contains("B NWG") || oldBloodGroup.Contains("B-NEG"))
                newBloodGroup = "B-";
            else if (oldBloodGroup.Contains("B=+") || oldBloodGroup.Contains("B POS") || oldBloodGroup.Contains("+B") || oldBloodGroup.Contains("B+") || oldBloodGroup.Contains("B +") || oldBloodGroup.Contains("BRH+") || oldBloodGroup.Contains("BRH +"))
                newBloodGroup = "B+";
            else if (oldBloodGroup.Contains("A+") || oldBloodGroup.Contains("A +") || oldBloodGroup.Contains("+A") || oldBloodGroup.Contains("ARH+") || oldBloodGroup.Contains("ARH +"))
                newBloodGroup = "A+";
            else if (oldBloodGroup.Contains("AB-") || oldBloodGroup.Contains("AB-NEG") || oldBloodGroup.Contains("AB NE"))
                newBloodGroup = "AB-";
            else if (oldBloodGroup.Contains("AB +") || oldBloodGroup.Contains("AB+") || oldBloodGroup.Contains("AB PO"))
                newBloodGroup = "AB+";
            else if (oldBloodGroup.Contains("0-") || oldBloodGroup.Contains("0 -") || oldBloodGroup.Contains("O -") || oldBloodGroup.Contains("O-") || oldBloodGroup.Contains("O NEG") || oldBloodGroup.Contains("ONEG") || oldBloodGroup.Contains("O-NEG"))
                newBloodGroup = "O-";
            else if (oldBloodGroup.Contains("0+") || oldBloodGroup.Contains("O+") || oldBloodGroup.Contains("O +") || oldBloodGroup.Contains("O++") || oldBloodGroup.Contains("ORH+") || oldBloodGroup.Contains("ORH +"))
                newBloodGroup = "O+";
            else
            {
                newBloodGroup = "NA";
                patientsWithBlankedBloodGroups.Add(new PatientWithChangedBloodGroup()
                {
                    PatientId = patientId,
                    OldBloodGroup = oldBloodGroup,
                    NewBloodGroup = newBloodGroup
                });
            }
            return newBloodGroup;
        }

        // Helper method to read data from access table
        private void GetAccessData(string query)
        {
            //// GETTING DATA FROM ACCESS 
            dataTable = new DataTable();
            using (OleDbConnection conn = new OleDbConnection(connectionStringForAccessDB))
            {
                using (OleDbDataAdapter da = new OleDbDataAdapter($"{query}", conn))
                    da.Fill(dataTable);
            }
        }

        // Helper method for complications
        private string IsValidMonth(string month)
        {
            if (string.IsNullOrWhiteSpace(month))
                return null;
            else if (month.ToLower() == "jan")
                return "01";
            else if (month.ToLower() == "feb")
                return "02";
            else if (month.ToLower() == "mar")
                return "03";
            else if (month.ToLower() == "apr")
                return "04";
            else if (month.ToLower() == "may")
                return "05";
            else if (month.ToLower() == "jun")
                return "06";
            else if (month.ToLower() == "jul")
                return "07";
            else if (month.ToLower() == "aug")
                return "08";
            else if (month.ToLower() == "sep")
                return "09";
            else if (month.ToLower() == "oct")
                return "10";
            else if (month.ToLower() == "nov")
                return "11";
            else if (month.ToLower() == "dec")
                return "12";
            else
                return null;
        }

        public async Task WriteChangedOrExceptionRecordsToFile()
        {
            try
            {
                string path = $"{AppDomain.CurrentDomain.BaseDirectory}RecordLogs";
                Directory.CreateDirectory(path);

                if (patientsWithBlankedBloodGroups != null && patientsWithBlankedBloodGroups.Count > 0)
                {
                    // write records with changed blood groups to file changed_blood_groups_patients.txt
                    string bloodgroupspath = $"{path}\\changed_blood_groups_patients.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(bloodgroupspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {patientsWithBlankedBloodGroups.Count}");
                        foreach (var patientrec in patientsWithBlankedBloodGroups)
                            await streamWriter.WriteLineAsync($"PatientId: {patientrec.PatientId} ### Old BloodGroup: {patientrec.OldBloodGroup} ### New BloodGroup: {patientrec.NewBloodGroup}");
                    }
                }

                if (patientsWithGeneralDepartments != null && patientsWithGeneralDepartments.Count > 0)
                {
                    // write records with changed departments to file changed_departments_patients.txt
                    string changeddepartmentspath = $"{path}\\changed_departments_patients.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(changeddepartmentspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {patientsWithGeneralDepartments.Count}");

                        foreach (var patientrec in patientsWithGeneralDepartments)
                            await streamWriter.WriteLineAsync($"PatientId: {patientrec.PatientId} ### Old Department: {patientrec.OldDepartment} ### New Department: {patientrec.NewDepartment}");
                    }
                }

                if (PatientRecordsWithExceptionsDict != null && PatientRecordsWithExceptionsDict.Count > 0)
                {
                    // write pat records with exceptions to file invalid_patients.txt
                    string invalidpatientspath = $"{ path}\\invalid_patients.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidpatientspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {PatientRecordsWithExceptionsDict.Count}");

                        foreach (var patientrec in PatientRecordsWithExceptionsDict)
                            await streamWriter.WriteLineAsync($"PatientId: {patientrec.Key} ### Exception: {patientrec.Value} \n \n");
                    }
                }

                if (MedicinesWithChangedValues != null && MedicinesWithChangedValues.Count > 0)
                {
                    string changedmedicinespath = $"{path}\\changed_medicines.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(changedmedicinespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {MedicinesWithChangedValues.Count}");

                        foreach (var medicinerec in MedicinesWithChangedValues)
                        {
                            string details = $"MedicineCode: {medicinerec.MedicineNo} ### MedicineId:{medicinerec.MedicineId} ### ";
                            if (!string.IsNullOrWhiteSpace(medicinerec.NewType))
                                details += $"Old Type: {medicinerec.OldType} ### New Type:{medicinerec.NewType}";
                            if (!string.IsNullOrWhiteSpace(medicinerec.NewGroup))
                                details += $"Old Group: {medicinerec.OldGroup} ### New Group:{medicinerec.NewGroup}";
                            if (!string.IsNullOrWhiteSpace(medicinerec.NewUnitOfMeasure))
                                details += $"Old UnitOfMeasure: {medicinerec.OldUnitOfMeasure} ### New UnitOfMeasure:{medicinerec.NewUnitOfMeasure}";
                            details += "\n \n";
                            await streamWriter.WriteLineAsync(details);
                        }
                    }
                }

                if (MedicineRecordsWithExceptionsDict != null && MedicineRecordsWithExceptionsDict.Count > 0)
                {
                    // write med records with exceptions to file invalid_medicines.txt
                    string invalidmedicinespath = $"{ path}\\invalid_medicines.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidmedicinespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {MedicineRecordsWithExceptionsDict.Count}");

                        foreach (var med in MedicineRecordsWithExceptionsDict)
                            await streamWriter.WriteLineAsync($"MedicineCode: {med.Key} ### Exception: {med.Value} \n \n");
                    }
                }

                if (PrescriptionRecordsWithExceptionsDict != null && PrescriptionRecordsWithExceptionsDict.Count > 0)
                {
                    // write presc records with exceptions to file invalid_prescriptions
                    string invalidprescriptionspath = $"{ path}\\invalid_prescriptions.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidprescriptionspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {PrescriptionRecordsWithExceptionsDict.Count}");

                        foreach (var presc in PrescriptionRecordsWithExceptionsDict)
                        {

                            string[] keyParts = presc.Key.Split('/');
                            await streamWriter.WriteLineAsync($"MedicineNo: {keyParts[0]} , PatientId: {keyParts[1]}, DtDate:{keyParts[2]} ### Exception: {presc.Value} \n \n");
                        }
                    }
                }

                if (PrescriptionCommentRecordsWithExceptionsDict != null && PrescriptionCommentRecordsWithExceptionsDict.Count > 0)
                {
                    // write presc records with exceptions to file invalid_prescriptions
                    string invalidprescriptionscommentpath = $"{ path}\\invalid_prescription_comments.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidprescriptionscommentpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {PrescriptionCommentRecordsWithExceptionsDict.Count}");

                        foreach (var presc in PrescriptionCommentRecordsWithExceptionsDict)
                        {

                            string[] keyParts = presc.Key.Split('/');
                            await streamWriter.WriteLineAsync($"PatientId: {keyParts[0]} , DtDate: {keyParts[1]} ### Exception: {presc.Value} \n \n");
                        }
                    }
                }

                if (EventRecordsWithExceptionsDict != null && EventRecordsWithExceptionsDict.Count > 0)
                {
                    string invalideventspath = $"{ path}\\invalid_events.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalideventspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {EventRecordsWithExceptionsDict.Count}");

                        foreach (var presc in EventRecordsWithExceptionsDict)
                        {

                            string[] keyParts = presc.Key.Split('/');
                            await streamWriter.WriteLineAsync($"PatientId: {keyParts[0]} , DOE: {keyParts[1]} , Event: {keyParts[2]} ### Exception: {presc.Value} \n \n");
                        }
                    }
                }

                if (IllnessRecordsWithExceptionsDict != null && IllnessRecordsWithExceptionsDict.Count > 0)
                {
                    string invalidillnesspath = $"{ path}\\invalid_illness.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidillnesspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {IllnessRecordsWithExceptionsDict.Count}");

                        foreach (var presc in IllnessRecordsWithExceptionsDict)
                        {

                            string[] keyParts = presc.Key.Split('/');
                            await streamWriter.WriteLineAsync($"PatientId: {keyParts[0]} , DOE: {keyParts[1]} , Illness: {keyParts[2]} ### Exception: {presc.Value} \n \n");
                        }
                    }
                }

                if (NewlyAddedComplicationCategories != null && NewlyAddedComplicationCategories.Count > 0)
                {
                    string newlyaddedcompcategoriespath = $"{ path}\\new_complication_categories.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(newlyaddedcompcategoriespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {NewlyAddedComplicationCategories.Count}");

                        foreach (var catg in NewlyAddedComplicationCategories)
                        {
                            await streamWriter.WriteLineAsync($"{catg} \n");
                        }
                    }
                }

                if (ComplicationMasterRecordsWithExceptionsDict != null && ComplicationMasterRecordsWithExceptionsDict.Count > 0)
                {
                    string invalidcompmasterpath = $"{ path}\\invalid_complication_master.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidcompmasterpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {ComplicationMasterRecordsWithExceptionsDict.Count}");

                        foreach (var compMaster in ComplicationMasterRecordsWithExceptionsDict)
                        {
                            string[] parts = compMaster.Key.Split('/');
                            await streamWriter.WriteLineAsync($"ComplicationCode: {parts[0]} ### Exception: {compMaster.Value} \n");
                        }
                    }
                }

                if (NewlyAddedSymptomCategories != null && NewlyAddedSymptomCategories.Count > 0)
                {
                    string newlyaddedsympcategoriespath = $"{ path}\\new_symptom_categories.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(newlyaddedsympcategoriespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {NewlyAddedSymptomCategories.Count}");

                        foreach (var catg in NewlyAddedSymptomCategories)
                        {
                            await streamWriter.WriteLineAsync($"{catg} \n");
                        }
                    }
                }

                if (SymptomMasterRecordsWithExceptionsDict != null && SymptomMasterRecordsWithExceptionsDict.Count > 0)
                {
                    string invalidsympmasterpath = $"{ path}\\invalid_symptom_master.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidsympmasterpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {SymptomMasterRecordsWithExceptionsDict.Count}");

                        foreach (var compMaster in SymptomMasterRecordsWithExceptionsDict)
                        {
                            string[] keyParts = compMaster.Key.Split('/');
                            await streamWriter.WriteLineAsync($"SymptomCode: {keyParts[0]} ### Exception: {compMaster.Value} \n");
                        }
                    }
                }

                if (NewlyAddedSignCategories != null && NewlyAddedSignCategories.Count > 0)
                {
                    string newlyaddedsigncategoriespath = $"{ path}\\new_sign_categories.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(newlyaddedsigncategoriespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {NewlyAddedSignCategories.Count}");

                        foreach (var catg in NewlyAddedSignCategories)
                        {
                            await streamWriter.WriteLineAsync($"{catg} \n");
                        }
                    }
                }

                if (SignMasterRecordsWithExceptionsDict != null && SignMasterRecordsWithExceptionsDict.Count > 0)
                {
                    string invalidsignmasterpath = $"{ path}\\invalid_sign_master.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidsignmasterpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {SignMasterRecordsWithExceptionsDict.Count}");

                        foreach (var signMaster in SignMasterRecordsWithExceptionsDict)
                        {
                            string[] keyParts = signMaster.Key.Split('/');
                            await streamWriter.WriteLineAsync($"SignCode: {keyParts[0]} ### Exception: {signMaster.Value} \n");
                        }
                    }
                }

                if (NewlyAddedTestCategories != null && NewlyAddedTestCategories.Count > 0)
                {
                    string newlyaddedtestcategoriespath = $"{ path}\\new_test_categories.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(newlyaddedtestcategoriespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {NewlyAddedTestCategories.Count}");

                        foreach (var catg in NewlyAddedTestCategories)
                        {
                            await streamWriter.WriteLineAsync($"{catg} \n");
                        }
                    }
                }

                if (InvestigationMasterRecordsWithExceptionsDict != null && InvestigationMasterRecordsWithExceptionsDict.Count > 0)
                {
                    string invalidinvestmasterpath = $"{ path}\\invalid_investigation_master.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidinvestmasterpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvestigationMasterRecordsWithExceptionsDict.Count}");

                        foreach (var test in InvestigationMasterRecordsWithExceptionsDict)
                        {
                            string[] keyParts = test.Key.Split('/');
                            await streamWriter.WriteLineAsync($"TestCode: {keyParts[0]} ### Exception: {test.Value} \n");
                        }
                    }
                }

                if (InvalidDeptTestPriorities != null && InvalidDeptTestPriorities.Count > 0)
                {
                    string invaliddepttestpath = $"{ path}\\invalid_dept_test_priorities.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invaliddepttestpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidDeptTestPriorities.Count}");

                        foreach (var item in InvalidDeptTestPriorities)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"TestCode: {keyParts[0]} Department: {keyParts[1]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidCatgTestPriorities != null && InvalidCatgTestPriorities.Count > 0)
                {
                    string invalidcatgtestpath = $"{ path}\\invalid_catg_test_priorities.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidcatgtestpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidCatgTestPriorities.Count}");

                        foreach (var item in InvalidCatgTestPriorities)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"TestCode: {keyParts[0]} Category: {keyParts[1]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidComplications != null && InvalidComplications.Count > 0)
                {
                    string invalidcomppath = $"{ path}\\invalid_complications.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidcomppath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidComplications.Count}");

                        foreach (var item in InvalidComplications)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"ComplicationCode: {keyParts[0]} Patient: {keyParts[1]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidSigns != null && InvalidSigns.Count > 0)
                {
                    string invalidsignspath = $"{ path}\\invalid_signs.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidsignspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidSigns.Count}");

                        foreach (var item in InvalidSigns)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"SignCode: {keyParts[0]} Patient: {keyParts[1]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidSymptoms != null && InvalidSymptoms.Count > 0)
                {
                    string invalidsymppath = $"{ path}\\invalid_symptoms.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidsymppath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidSymptoms.Count}");

                        foreach (var item in InvalidSymptoms)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"SymptomCode: {keyParts[0]} Patient: {keyParts[1]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidInvestigations != null && InvalidInvestigations.Count > 0)
                {
                    string invalidinvestigationspath = $"{ path}\\invalid_investigations.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidinvestigationspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidInvestigations.Count}");

                        foreach (var item in InvalidInvestigations)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"TestCode: {keyParts[0]} Patient: {keyParts[1]} TestDate:{keyParts[2]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if (InvalidForms != null && InvalidForms.Count > 0)
                {
                    string invalidformspath = $"{ path}\\invalid_forms.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(invalidformspath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {InvalidForms.Count}");

                        foreach (var item in InvalidForms)
                        {
                            string[] keyParts = item.Key.Split('/');
                            await streamWriter.WriteLineAsync($"FormFormatName: {keyParts[0]} Patient: {keyParts[1]} FormDate:{keyParts[2]} ### Exception: {item.Value} \n");
                        }
                    }
                }

                if(DuplicatePrescriptionsSetWithDeletedDate != null && DuplicatePrescriptionsSetWithDeletedDate.Count >0)
                {
                    string duplicateprescpath = $"{ path}\\duplicate_prescriptions_set_with_deleteddate.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(duplicateprescpath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {DuplicatePrescriptionsSetWithDeletedDate.Count}");

                        foreach (var item in DuplicatePrescriptionsSetWithDeletedDate)
                        {
                            await streamWriter.WriteLineAsync($"{item} \n");
                        }
                    }
                }

                if (DuplicateInvestigationsSetWithDeletedDate != null && DuplicateInvestigationsSetWithDeletedDate.Count > 0)
                {
                    string duplicateinvespath = $"{ path}\\duplicate_investigations_set_with_deleteddate.txt";
                    using (StreamWriter streamWriter = new StreamWriter(File.Open(duplicateinvespath, FileMode.Create)))
                    {
                        await streamWriter.WriteLineAsync($"Total Records: {DuplicateInvestigationsSetWithDeletedDate.Count}");

                        foreach (var item in DuplicateInvestigationsSetWithDeletedDate)
                        {
                            await streamWriter.WriteLineAsync($"{item} \n");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        #endregion

        public async Task PortLocations()
        {
            try
            {
                // Note - There are no locations in the access DB. This is a new functionality that we have added. Hence we don't use AccessDB for this.
                // We are manually inserting hardcoded locations - DoctorsHouse - 1, Satna -2 
                /*
                 LocationId
                 LocationName
                 LocationCreatedDate     (set with default createddate)
                 DateOfSyncToCloud
                 DateOfSyncFromCloud
                 SyncToCloudStatus      (currently not used)
                 SyncFromCloudStatus    (currently not used)
                 DateTillWhichRecordsFetchedForSyncFromCloud
                 */

                // During SyncToCloud -   Condition for dates is - where locationId = @locid and CreatedDate > @fromdate && CreatedDate <= @todate and LocationId = @locationid
                // where toDate = DateTime.Now
                // fromDate = await syncRepository.GetLatestSyncToDate();

                // During SyncFromCloud - Condition for dates is - where locationId = @locid and CreatedDate > DateTillWhichRecordsFetchedForSyncFromCloud and CreatedDate <= DateOfSyncToCloud
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into locations(LocationId,LocationName,LocationCreatedDate,DateOfSyncToCloud,DateOfSyncFromCloud,DateTillWhichRecordsFetchedForSyncFromCloud) values" +
                                               $"(1,'DoctorsHouse','{defaultCreatedDate}','{new DateTime().AddYears(1900).AddDays(1).ToString("yyyy-MM-dd hh:mm:ss")}','{new DateTime().AddYears(1900).AddDays(1).ToString("yyyy-MM-dd hh:mm:ss")}','{new DateTime().AddYears(1900).ToString("yyyy-MM-dd hh:mm:ss")}')," +
                                               $"(2,'Satna','{defaultCreatedDate}','{new DateTime().AddYears(1900).AddDays(1).ToString("yyyy-MM-dd hh:mm:ss")}','{new DateTime().AddYears(1900).AddDays(1).ToString("yyyy-MM-dd hh:mm:ss")}','{new DateTime().AddYears(1900).ToString("yyyy-MM-dd hh:mm:ss")}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortRoles()
        {
            // Note - There are no roles in the access DB. This is a new functionality that we have added. Hence we don't use AccessDB for this.
            // We are inserting hardcoded roles - Admin (for the developers), Viewer (can only view), Editor (Viewer permissions + can save everything except mastertables), AppMaster (Editor permissions + save mastertables + sync)
            /*
             RoleId
             RoleName
             LocationId
             CreatedDate
             UpdatedDate
             DeletedDate
             AppMasterUserId
            */

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into roles(RoleId,RoleName,LocationId,CreatedDate,UpdatedDate,DeletedDate) values" +
                                               $"('{Roles["Admin"]}','Admin',{locationId},'{defaultCreatedDate}',null,null)," +
                                               $"('{Roles["Viewer"]}','Viewer',{locationId},'{defaultCreatedDate}',null,null)," +
                                               $"('{Roles["Editor"]}','Editor',{locationId},'{defaultCreatedDate}',null,null)," +
                                               $"('{Roles["AppMaster"]}','AppMaster',{locationId},'{defaultCreatedDate}',null,null)";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortUsers()
        {
            // Note - There are no users in the access DB. This is a new functionality that we have added. Hence we don't use AccessDB for this.
            // We are manually inserting hardcoded users - Leadows (Admin, leadows123%), xx (Editor), xxx (AppMaster)
            /*
            UserId
            UserName
            Password
            Salt
            PhoneNumber
            Email
            LocationId
            CreatedDate
            UpdatedDate
            DeletedDate
            AppMasterUserId
            IsActive
            IsLoggedIn
            FullName
            Designation
            */

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into users(UserId,UserName,Password,Salt,PhoneNumber,Email,LocationId,CreatedDate,UpdatedDate,DeletedDate,AppMasterUserId,IsActive,IsLoggedIn) values" +
                                               $"('{adminUserId}','Leadows','0y+83va93bipRBy7DZYxB7q9+abNuR11ZORzSvxuDS8=','Jn1p9JnmrS9eTenPIcuqDg==',null,'xx',{locationId},'{defaultCreatedDate}', null,null,null,1,0)," +
                                               $"('{ Users["xxx"]}','xxx','4RkZTpRkaE91Nk4vIqKZinYU/ueZ71cI0MqVg7KhppE=','/gZvp1txrEF46lKUsaSXlw==',null,'xxx',{locationId},'{defaultCreatedDate}', null,null,null,1,0)," +
                                               $"('{ Users["Ashay"]}','xx','FLSaEUlBLLcN13v2UuMuxvC3jZpwpd0X+dZWVZU2GPg=','7NVOp1x5UC/lTw44Tatyzg==',null,'xxx',{locationId},'{defaultCreatedDate}', null,null,null,1,0)";


                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortUsersInRoles()
        {
            // Note - There are no users in the access DB. This is a new functionality that we have added. Hence we don't use AccessDB for this.
            // We are manually inserting usersinroles - Leadows (Admin), xx (Editor), xx (AppMaster)
            /*
            UserId
            RoleId
            LocationId
            CreatedDate
            UpdatedDate
            DeletedDate
            AppMasterUserId
            */
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into usersinroles(UserId,RoleId,LocationId,CreatedDate,UpdatedDate,DeletedDate,AppMasterUserId) values" +
                                               $"('{Users["Leadows"]}','{Roles["Admin"]}',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Users["xxx"]}','{Roles["AppMaster"]}',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ Users["xxx"]}','{Roles["Editor"]}',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortMedicineGroups()
        {
            // There is no table for medicine groups in access. Hence we are populating by getting all the unique groups that have been used in the medicines table.
            // Also, we have been given a final list of groups that should be used. Hence all the incorrect entries in the medicine table have been mapped to these groups in the hardcoded dictionary
            /*
             GroupId       
             GroupName
             Colour
             LocationId
             CreatedDate
             UpdatedDate
             DeletedDate
             UserId
             */

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into medicinegroups(GroupId,GroupName,Colour,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{MedicineGroups["AKT"].Id}','{MedicineGroups["AKT"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["ANTIBIOTIC"].Id}','{MedicineGroups["ANTIBIOTIC"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["CNS"].Id}','{MedicineGroups["CNS"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["CVS"].Id}','{MedicineGroups["CVS"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["DIAB"].Id}','{MedicineGroups["DIAB"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["GEN"].Id}','{MedicineGroups["GEN"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["HD"].Id}','{MedicineGroups["HD"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["HTN"].Id}','{MedicineGroups["HTN"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["NEPHRO"].Id}','{MedicineGroups["NEPHRO"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["TB"].Id}','{MedicineGroups["TB"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{MedicineGroups["TRANSPLANT"].Id}','{MedicineGroups["TRANSPLANT"].Name}','LightGray',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortPatientDepartments()
        {
            // There is no departments table in accessDB. We are adding the departments based on the list given by the doctor and the entries in the department column of patients table of accessDB
            // Manually inserting the departments - DIAB, TRANSPLANT, NEPHRO, GENERAL
            /* 
               DepartmentId
               DepartmentName
               Colour
               Priority
               LocationId
               CreatedDate
               UpdatedDate
               DeletedDate
               UserId 
             */

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into departments(DepartmentId,DepartmentName,Colour,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{Departments["DIAB"]}','DIAB','LightGray',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','TRANSPLANT','LightGray',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','NEPHRO','LightGray',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','GENERAL','LightGray',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortShortcutDataEntryTypes()
        {
            // There is no table in AccessDB which contains entries for the dataentrytype. These have been made columns in the shortcutkeys table. 
            // Since we wanted the provision to be able to add more dataentrytypes if required, without changing the code - so we have created this table
            // We will manually insert the values in it (taken from columnnames of shortcutkeys) - Patient Form,Medicine Dosage,Test,Illness,Events
            // Note - These names have to be exactly as mentioned above since they have been hardcoded in the checks of those particular forms. If we change the name then we need to change the code as well.

            /*
             Id
             DataEntryType
             CreatedDate
             UpdatedDate
             DeletedDate
             LocationId
             UserId
             */


            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into shortcutdataentrytypes(Id,DataEntryType,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{ShortcutDataEntryTypes["Patient Form"]}','Patient Form',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutDataEntryTypes["Medicine Dosage"]}','Medicine Dosage',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutDataEntryTypes["Test"]}','Test',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutDataEntryTypes["Illness"]}','Illness',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutDataEntryTypes["Events"]}','Events',{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortShortcutKeys()
        {
            // There is no table in AccessDB which contains entries for which keys can be used as shortcutkeys. These have been hardcoded in the shortcutkeys table. 
            // Since we wanted the provision to be able to add more keys if required, without changing the code - so we have created this table
            // We will manually insert the values in it (taken from the hardcoded values of shortcutkeys) - F1 to F12

            /*
                Id
                KeyName
                CreatedDate
                UpdatedDate
                DeletedDate
                LocationId
                UserId
             */

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into shortcutkeys(Id,KeyName,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{ShortcutKeys["F1"]}','F1',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F2"]}','F2',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F3"]}','F3',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F4"]}','F4',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F5"]}','F5',5,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F6"]}','F6',6,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F7"]}','F7',7,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F8"]}','F8',8,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F9"]}','F9',9,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F10"]}','F10',10,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F11"]}','F11',11,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{ShortcutKeys["F12"]}','F12',12,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortDeptGroupPriorities()
        {
            // Each group is to be mapped to each department. 
            // Read from the dept_catg_priority table from the accessDB where department names are those that exist as keys of departments dictionary.
            // For now being hardcoded
            /*
             DepartmentId
             GroupId
             Priority
             LocationId
             CreatedDate
             UpdatedDate
             DeletedDate
             UserId
              */

            /* Hardcoding values 
             dept	grp	  new priority
            DIAB	DIAB	1
            DIAB	HTN	    2
            DIAB	CVS	    3
            DIAB	NEPHRO	4
            DIAB	GEN	 	5
            DIAB	AKT	    6
            DIAB	ANTIBIOTIC		7
            DIAB	HD		8
            DIAB	TRANSPLANT		9
            DIAB	CNS		10
            DIAB	TB		11
            */
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into dept_group_priorities(DepartmentId,GroupId,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["DIAB"].Id}',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["HTN"].Id}',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["CVS"].Id}',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["NEPHRO"].Id}',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["GEN"].Id}',5,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["AKT"].Id}',6,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["ANTIBIOTIC"].Id}',7,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["HD"].Id}',8,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["TRANSPLANT"].Id}',9,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["CNS"].Id}',10,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["DIAB"]}','{MedicineGroups["TB"].Id}',11,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }


                /*
                NEPHRO	TRANSPLANT	1
                NEPHRO	NEPHRO	2
                NEPHRO	HTN	    3
                NEPHRO	HD		4
                NEPHRO	CVS		5
                NEPHRO	DIAB	6	
                NEPHRO	GEN		7
                NEPHRO	AKT		8
                NEPHRO	ANTIBIOTIC	9
                NEPHRO	CNS		10
                NEPHRO	TB		11
                */

                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into dept_group_priorities(DepartmentId,GroupId,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["TRANSPLANT"].Id}',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["NEPHRO"].Id}',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["HTN"].Id}',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["HD"].Id}',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["CVS"].Id}',5,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["DIAB"].Id}',6,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["GEN"].Id}',7,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["AKT"].Id}',8,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["ANTIBIOTIC"].Id}',9,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["CNS"].Id}',10,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["NEPHRO"]}','{MedicineGroups["TB"].Id}',11,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }

                /*
                TRANSPLANT	TRANSPLANT	1
                TRANSPLANT	NEPHRO	2	    
                TRANSPLANT	HTN		3
                TRANSPLANT	HD		4
                TRANSPLANT	DIAB	5	    
                TRANSPLANT	AKT		6
                TRANSPLANT	ANTIBIOTIC	7
                TRANSPLANT	CNS		8
                TRANSPLANT	CVS		9
                TRANSPLANT	GEN		10
                TRANSPLANT	TB		11
                */

                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into dept_group_priorities(DepartmentId,GroupId,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["TRANSPLANT"].Id}',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["NEPHRO"].Id}',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["HTN"].Id}',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["HD"].Id}',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["DIAB"].Id}',5,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["AKT"].Id}',6,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["ANTIBIOTIC"].Id}',7,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["CNS"].Id}',8,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["CVS"].Id}',9,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["GEN"].Id}',10,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["TRANSPLANT"]}','{MedicineGroups["TB"].Id}',11,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }

                /*
                GEN	TRANSPLANT		1
                GEN	NEPHRO	2
                GEN	HTN		3
                GEN	HD		4
                GEN	DIAB	5
                GEN	CVS		6
                GEN	AKT		7
                GEN	TB		8
                GEN	GEN		9
                GEN	CNS		10
                GEN	ANTIBIOTIC	11
                 */

                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into dept_group_priorities(DepartmentId,GroupId,Priority,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["TRANSPLANT"].Id}',1,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["NEPHRO"].Id}',2,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["HTN"].Id}',3,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["HD"].Id}',4,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["DIAB"].Id}',5,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["CVS"].Id}',6,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["AKT"].Id}',7,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["TB"].Id}',8,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["GEN"].Id}',9,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["CNS"].Id}',10,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')," +
                                               $"('{Departments["GENERAL"]}','{MedicineGroups["ANTIBIOTIC"].Id}',11,{locationId},'{defaultCreatedDate}', null,null,'{adminUserId}')";

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortShortcuts()
        {
            // Note - The code to port shortcuts will take into account any new keys added to the table. BUT IT WILL NOT TAKE INTO ACCOUNT ANY NEW COLUMNS ADDED.
            // Columns being ported are - strASCII_CODE, strPatient, strMedicine, strTest, strEvents, strillness

            // Read from the shortcuts table in access and save to mysql
            /* 
             Id
             KeyId
             DataEntryTypeId
             Text
             LocationId
             CreatedDate
             UpdatedDate
             UserId
             */

            // Note - if more columns have been added, then the accessDB query and Mysql queries need to be changed.
            GetAccessData("select strASCII_CODE, strPatient, strMedicine, strTest, strEvents, strillness from ShortCutKeys;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            command.Parameters.Clear();

                            // First column in shortcuts table is ascii code  112 (F1) - 123 (F12)
                            int key = Convert.ToInt32(dataTable.Rows[i][0]);
                            string keyId = await GetKeyId(key);
                            command.Parameters.AddWithValue("@keyid", keyId);

                            command.CommandText = "insert into shortcuts(Id,KeyId,DataEntryTypeId,Text,LocationId,CreatedDate,UpdatedDate,UserId) values" +
                                                  $"('{Guid.NewGuid().ToString()}',@keyid,'{ShortcutDataEntryTypes["Patient Form"]}','{dataTable.Rows[i][1].ToString().Trim()}',{locationId},'{defaultCreatedDate}',null,'{adminUserId}')," +
                                                  $"('{Guid.NewGuid().ToString()}',@keyid,'{ShortcutDataEntryTypes["Medicine Dosage"]}','{dataTable.Rows[i][2].ToString().Trim()}',{locationId},'{defaultCreatedDate}',null,'{adminUserId}')," +
                                                  $"('{Guid.NewGuid().ToString()}',@keyid,'{ShortcutDataEntryTypes["Test"]}','{dataTable.Rows[i][3].ToString().Trim()}',{locationId},'{defaultCreatedDate}',null,'{adminUserId}')," +
                                                  $"('{Guid.NewGuid().ToString()}',@keyid,'{ShortcutDataEntryTypes["Illness"]}','{dataTable.Rows[i][4].ToString().Trim()}',{locationId},'{defaultCreatedDate}',null,'{adminUserId}')," +
                                                  $"('{Guid.NewGuid().ToString()}',@keyid,'{ShortcutDataEntryTypes["Events"]}','{dataTable.Rows[i][5].ToString().Trim()}',{locationId},'{defaultCreatedDate}',null,'{adminUserId}')";

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortPatients()
        {
            // Read from accessdb 
            /*
             PatientId          - numPatientid
             PatientName        - fullname
             Gender             - strSex
             DateOfBirth        - dtDOB
             BloodGroup         - strBldGrp
             DiagnosedYear      - strDiagnosedDate
             Height             - Height
             Weight             - weight
             DepartmentId       - department   (for given department name get id from dict)
             Department         -  null
             DeptGroup          - strgroup 
             Diagnosis          - strDiagnosis
             Operations         - strOperation
             OtherIllnesses     - strIllnesses
             Habits             - strHabbits
             FamilyHistory      - strFamilyHistory
             RefByDoctor        - strRefbyDoc
             Address            - area
             City               - city
             State              - state
             Country            - country
             Pin                - zip
             PhoneNumber1       - phone_home
             PhoneNumber2       - phone_office
             EmailAddress       - email_id
             Cr                 - cr
             BookMark           - ?? should have the search bookmarks   (for now save null)
             LastVisit          - last_visit 
             Comments           - strComplications  (as per doctor's mail 14th Nov, 2019)
             LocationId         - ! locid!
             CreatedDate        - !createddate!
             UpdatedDate        - Mod_Date
             DeletedDate        - null
             UserId             - !adminid!
             ImageId            - null
             */


            GetAccessData("select numPatientid, fullname, strSex, dtDOB, strBldGrp, strDiagnosedDate, Height, weight, department, strgroup, strDiagnosis, strComplications, strOperation, strIllnesses, " +
                          "strHabbits, strFamilyHistory, strRefbyDoc, area, city, state, country, zip, phone_home, phone_office, email_id, cr,last_visit,Mod_Date from Patients;");

            using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
            {
                using (MySqlCommand command = new MySqlCommand())
                {
                    await connection.OpenAsync();
                    command.Connection = connection;

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string patientInfo = string.Empty;
                        try
                        {
                            command.Parameters.Clear();

                            command.CommandText = "insert into patients(PatientId,PatientName,Gender,DateOfBirth,BloodGroup,DiagnosedYear,Height,Weight,DepartmentId,Tags,Diagnosis,Operations,OtherIllnesses," +
                                                  "Habits,FamilyHistory,RefByDoctor,Address,City,State,Country,Pin,PhoneNumber1,PhoneNumber2,EmailAddress,BookMark, LastVisit,Comments,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId,ImageId) values" +
                                                  $"(@patientid,@patientname,@gender,@dateofbirth,@bloodgroup,@diagnosedyear,@height,@weight,@departmentid,@patienttag,@diagnosis,@operations,@otherillnesses,@habits,@familyhistory,@refbydoctor," +
                                                  $"@address,@city,@state,@country,@pin,@phonenumber1,@phonenumber2,@emailaddress,null,@lastvisit,@comments,{locationId},'{defaultCreatedDate}',@updateddate,null,'{adminUserId}',null)";

                            patientInfo = "insert into patients(PatientId, PatientName, Gender, DateOfBirth, BloodGroup, DiagnosedYear, Height, Weight, DepartmentId, Tags, Diagnosis,Operations, OtherIllnesses," +
                                "Habits,FamilyHistory,RefByDoctor,Address,City,State,Country,Pin,PhoneNumber1,PhoneNumber2,EmailAddress,BookMark, LastVisit,Comments,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId,ImageId) values (";

                            int patientId = Convert.ToInt32(dataTable.Rows[i][0]);
                            command.Parameters.AddWithValue("@patientid", patientId);      // cannot be null
                            patientInfo += $"{patientId},";

                            command.Parameters.AddWithValue("@patientname", MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim()));           // cannot be null
                            patientInfo += $"'{MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim())}',";

                            string gender = MySqlHelper.EscapeString(MySqlHelper.EscapeString(dataTable.Rows[i][2] == DBNull.Value ? null : dataTable.Rows[i][2].ToString().Trim()));
                            command.Parameters.AddWithValue("@gender", gender);
                            patientInfo += $"'{gender}',";

                            string dateOfBirth = dataTable.Rows[i][3] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][3])).ToString("yyyy-MM-dd hh:mm:ss");

                            command.Parameters.AddWithValue("@dateofbirth", dateOfBirth);
                            patientInfo += $"'{dateOfBirth}',";


                            // Incase of blood group - some of the values have been mapped to the expected types. The rest will go as null
                            // This is on the basis of the bloodgroup mapping in DataPortingSheet_v1.xlsx
                            string oldBloodGroup = dataTable.Rows[i][4] == DBNull.Value ? null : dataTable.Rows[i][4].ToString().Trim();
                            string newBloodGroup = GetValidBloodGroup(oldBloodGroup, patientId);
                            command.Parameters.AddWithValue("@bloodgroup", newBloodGroup);
                            patientInfo += $"'{newBloodGroup}',";

                            string diagnosedYear = MySqlHelper.EscapeString(dataTable.Rows[i][5] == DBNull.Value ? null : dataTable.Rows[i][5].ToString().Trim());
                            command.Parameters.AddWithValue("@diagnosedyear", diagnosedYear);
                            patientInfo += $"'{diagnosedYear}',";

                            string height = MySqlHelper.EscapeString(dataTable.Rows[i][6] == DBNull.Value ? null : dataTable.Rows[i][6].ToString().Trim());
                            command.Parameters.AddWithValue("@height", height);
                            patientInfo += $"'{height}',";

                            string weight = MySqlHelper.EscapeString(dataTable.Rows[i][7] == DBNull.Value ? null : dataTable.Rows[i][7].ToString().Trim());
                            command.Parameters.AddWithValue("@weight", weight);
                            patientInfo += $"'{weight}',";

                            // For given department -> check if it belongs to the departments dict keys. if it does then use as key to get value.
                            // else use GENERAL. This is on the basis of the bloodgroup mapping in DataPortingSheet_v1.xlsx 
                            string oldDepartmentName = MySqlHelper.EscapeString(dataTable.Rows[i][8] == DBNull.Value ? null : dataTable.Rows[i][8].ToString().Trim());
                            string newDepartmentId = GetValidDepartment(oldDepartmentName, patientId);
                            command.Parameters.AddWithValue("@departmentid", newDepartmentId);
                            patientInfo += $"'{newDepartmentId}',";
                            patientInfo += $"null,";

                            string patienttag = MySqlHelper.EscapeString(dataTable.Rows[i][9] == DBNull.Value ? null : dataTable.Rows[i][9].ToString().Trim());
                            command.Parameters.AddWithValue("@patienttag", patienttag);
                            patientInfo += $"'{patienttag}',";

                            string diagnosis = MySqlHelper.EscapeString(dataTable.Rows[i][10] == DBNull.Value ? null : dataTable.Rows[i][10].ToString().Trim());
                            command.Parameters.AddWithValue("@diagnosis", diagnosis);
                            patientInfo += $"'{diagnosis}',";

                            string operations = MySqlHelper.EscapeString(dataTable.Rows[i][12] == DBNull.Value ? null : dataTable.Rows[i][12].ToString().Trim());
                            command.Parameters.AddWithValue("@operations", operations);
                            patientInfo += $"'{operations}',";

                            string otherillnesses = MySqlHelper.EscapeString(dataTable.Rows[i][13] == DBNull.Value ? null : dataTable.Rows[i][13].ToString().Trim());
                            command.Parameters.AddWithValue("@otherillnesses", otherillnesses);
                            patientInfo += $"'{otherillnesses}',";

                            string habits = MySqlHelper.EscapeString(dataTable.Rows[i][14] == DBNull.Value ? null : dataTable.Rows[i][14].ToString().Trim());
                            command.Parameters.AddWithValue("@habits", habits);
                            patientInfo += $"'{habits}',";

                            string familyhistory = MySqlHelper.EscapeString(dataTable.Rows[i][15] == DBNull.Value ? null : dataTable.Rows[i][15].ToString().Trim());
                            command.Parameters.AddWithValue("@familyhistory", familyhistory);
                            patientInfo += $"'{familyhistory}',";

                            string refbydoctor = MySqlHelper.EscapeString(dataTable.Rows[i][16] == DBNull.Value ? null : dataTable.Rows[i][16].ToString().Trim());
                            command.Parameters.AddWithValue("@refbydoctor", refbydoctor);
                            patientInfo += $"'{refbydoctor}',";

                            string address = MySqlHelper.EscapeString(dataTable.Rows[i][17] == DBNull.Value ? null : dataTable.Rows[i][17].ToString().Trim());
                            command.Parameters.AddWithValue("@address", address);
                            patientInfo += $"'{address}',";

                            string city = MySqlHelper.EscapeString(dataTable.Rows[i][18] == DBNull.Value ? null : dataTable.Rows[i][18].ToString().Trim());
                            command.Parameters.AddWithValue("@city", city);
                            patientInfo += $"'{city}',";

                            string state = MySqlHelper.EscapeString(dataTable.Rows[i][19] == DBNull.Value ? null : dataTable.Rows[i][19].ToString().Trim());
                            command.Parameters.AddWithValue("@state", state);
                            patientInfo += $"'{state}',";

                            string country = MySqlHelper.EscapeString(dataTable.Rows[i][20] == DBNull.Value ? null : dataTable.Rows[i][20].ToString().Trim());
                            command.Parameters.AddWithValue("@country", country);
                            patientInfo += $"'{country}',";

                            string pin = MySqlHelper.EscapeString(dataTable.Rows[i][21] == DBNull.Value ? null : dataTable.Rows[i][21].ToString().Trim());
                            command.Parameters.AddWithValue("@pin", pin);
                            patientInfo += $"'{pin}',";

                            string phonenumber1 = MySqlHelper.EscapeString(dataTable.Rows[i][22] == DBNull.Value ? null : dataTable.Rows[i][22].ToString().Trim());
                            command.Parameters.AddWithValue("@phonenumber1", phonenumber1);
                            patientInfo += $"'{phonenumber1}',";

                            string phonenumber2 = MySqlHelper.EscapeString(dataTable.Rows[i][23] == DBNull.Value ? null : dataTable.Rows[i][23].ToString().Trim());
                            command.Parameters.AddWithValue("@phonenumber2", phonenumber2);
                            patientInfo += $"'{phonenumber2}',";

                            string emailaddress = MySqlHelper.EscapeString(dataTable.Rows[i][24] == DBNull.Value ? null : dataTable.Rows[i][24].ToString().Trim());
                            command.Parameters.AddWithValue("@emailaddress", emailaddress);
                            patientInfo += $"'{emailaddress}',";

                            string lastvisit = dataTable.Rows[i][26] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][26])).ToString("yyyy-MM-dd hh:mm:ss");
                            command.Parameters.AddWithValue("@lastvisit", lastvisit);
                            patientInfo += $"'{lastvisit}',";

                            // Complications field holds comments in accessDB
                            string comments = MySqlHelper.EscapeString(dataTable.Rows[i][11] == DBNull.Value ? null : dataTable.Rows[i][11].ToString().Trim());
                            command.Parameters.AddWithValue("@comments", comments);
                            patientInfo += $"'{comments}',";
                            patientInfo += $"{locationId},'{defaultCreatedDate}',";

                            string updateddate = dataTable.Rows[i][27] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][27])).ToString("yyyy-MM-dd hh:mm:ss");
                            command.Parameters.AddWithValue("@updateddate", updateddate);
                            patientInfo += $"'{updateddate}',";
                            patientInfo += $"null,'{adminUserId}',null);";

                            int x = await command.ExecuteNonQueryAsync();
                            if (x >= 1)
                                PatientIds.Add(patientId);
                        }
                        catch (Exception ex)
                        {
                            PatientRecordsWithExceptionsDict.Add(Convert.ToString(dataTable.Rows[i][0]), patientInfo);
                        }
                    }
                }
            }
        }

        public async Task PortPatientTags()
        {
            // The patient tags table represents the grpmaster table in AccessDB. However since the doctor's list of groups is different from the entries in this table, 
            // hence we will be manually adding the tags. Refer DataPortingSheet_v1.xlsx PatientTags Sheet.
            // The tags have been added to patienttags.txt in PortData folder (make sure there are no blank lines at the end of the file)
            // one tag per line - this is read and added to DB
            // Note - There is also a postbuild event to move the PortData folder to the bin folder. 

            /*
             TagName
             LocationId
             CreatedDate
             UpdatedDate
             DeletedDate
             UserId
            */

            try
            {
                List<string> tags = new List<string>();
                string path = $"{AppDomain.CurrentDomain.BaseDirectory}PortData//patienttags.txt";
                using (StreamReader streamReader = new StreamReader(File.Open(path, FileMode.Open)))
                {
                    while (!streamReader.EndOfStream)
                    {
                        string tag = await streamReader.ReadLineAsync();
                        // This check has been added to make sure blank lines do not get added as new entries in the table
                        if (!string.IsNullOrWhiteSpace(tag))
                            tags.Add(tag);
                    }
                }

                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        foreach (string tagName in tags)
                        {
                            command.CommandText = "insert into patienttags(TagName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                                  $"('{tagName}',{locationId},'{defaultCreatedDate}',null,null,'{adminUserId}');";

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortFormFormats()
        {
            // Form Formats are taken from formFormats table in accessdb
            /*
             FormFormatId
             FormName           form_name
             FormFormatStr      form_format
             CreatedDate
             UpdatedDate        Mod_Date
             DeletedDate
             LocationId
             UserId
             */

            GetAccessData("select form_name,form_format,Mod_Date from formFormats;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            try
                            {
                                command.Parameters.Clear();

                                string formName = dataTable.Rows[i][0].ToString().Trim();
                                FormFormats.Add(formName.ToUpper(), Guid.NewGuid().ToString());

                                command.Parameters.AddWithValue("@formformatstr", MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim()));
                                command.Parameters.AddWithValue("@moddate", dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss"));

                                command.CommandText = "insert into formformats(FormFormatId,FormName,FormFormatStr,CreatedDate,UpdatedDate,DeletedDate,LocationId,UserId) values" +
                                                  $"('{FormFormats[formName.ToUpper()]}','{formName}',@formformatstr,'{defaultCreatedDate}',@moddate,null,{locationId},'{adminUserId}');";

                                await command.ExecuteNonQueryAsync();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortMedicines()
        {
            // Medicines are ported from MedicineMaster table in accessDB
            /*
            MedicineId              !generate guid!
            GroupId                 department - map name to medicinegroup key and set the value (id)
            MedicineName            strMedicineName
            Power                   strPower
            Type                    cType   - only from list - Capsule, Injection, Syrup, Tablet, Misc
            Description             strDescription
            Mfgr                    cMfgr
            UnitOfMeasure           numUnitofMeasure
            Priority                priority
            Stock                   0 (as per doctors communication) // ostock
            Issue                   0 (as per doctors communication) //issue
            Rate                    rate    
            Graph_required          0 by default 
            MedicineCode            ---- not to be ported --- 
            LocationId
            CreatedDate
            UpdatedDate             Mod_Date
            DeletedDate
            UserId
            */
            MedicinesWithChangedValues = new List<MedicineWithChangedValues>();
            // we will get the value of medicine code and save in dictionary as key with value as the newly generated guid. This will be required in prescriptions.
            GetAccessData("select iMedicineNo,department,strMedicineName,strPower,cType,strDescription,cMfgr,numUnitofMeasure,priority,ostock,issue,rate,Mod_Date from MedicineMaster;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            try
                            {
                                MedicineWithChangedValues changedRecord = new MedicineWithChangedValues();
                                command.Parameters.Clear();

                                command.CommandText = "insert into medicines(MedicineId,GroupId,MedicineName,Power,Type,Description,Mfgr,UnitOfMeasure,Priority,Stock,Issue,Rate,Graph_required,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                                 $"(@medicineid,@groupid,@medicinename,@power,@type,@description,@mfgr,@unitofmeasure,@priority,@stock,@issue,@rate,0,{locationId},'{defaultCreatedDate}',@moddate,null,'{adminUserId}');";

                                int medicineNo = Convert.ToInt32(dataTable.Rows[i][0]);
                                string guid = Guid.NewGuid().ToString();
                                command.Parameters.AddWithValue("@medicineid", guid);
                                changedRecord.MedicineNo = medicineNo;
                                changedRecord.MedicineId = guid;

                                string group = dataTable.Rows[i][1] == DBNull.Value ? null : dataTable.Rows[i][1].ToString().Trim();
                                if (MedicineGroups.ContainsKey(group.ToUpper()))
                                    command.Parameters.AddWithValue("@groupid", MedicineGroups[group.ToUpper()].Id);
                                else
                                {
                                    changedRecord.OldGroup = group;
                                    changedRecord.NewGroup = "GEN";
                                    command.Parameters.AddWithValue("@groupid", MedicineGroups["GEN"].Id);
                                }

                                command.Parameters.AddWithValue("@medicinename", dataTable.Rows[i][2] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim()));

                                command.Parameters.AddWithValue("@power", dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim()));

                                string type = dataTable.Rows[i][4] == DBNull.Value ? null : dataTable.Rows[i][4].ToString().Trim();
                                string oldType = type;
                                if (oldType == null)
                                {
                                    changedRecord.OldType = oldType;
                                    changedRecord.NewType = "Misc";
                                    type = "Misc";
                                }
                                else
                                {
                                    type = type.Trim().ToLower();
                                    if (type.Equals("cap") || type.Equals("capsule"))
                                        type = "Capsule";
                                    else if (type.Equals("tab") || type.Equals("tablet"))
                                        type = "Tablet";
                                    else if (type.Equals("injection"))
                                        type = "Injection";
                                    else if (type.Equals("syrup"))
                                        type = "Syrup";
                                    else if (type.Equals("misc"))
                                        type = "Misc";
                                    else
                                    {
                                        changedRecord.OldType = oldType;
                                        changedRecord.NewType = "Misc";
                                        type = "Misc";
                                    }
                                }

                                command.Parameters.AddWithValue("@type", type);

                                command.Parameters.AddWithValue("@description", dataTable.Rows[i][5] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][5].ToString().Trim()));

                                command.Parameters.AddWithValue("@mfgr", dataTable.Rows[i][6] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][6].ToString().Trim()));

                                string unitOfMeasure = dataTable.Rows[i][7] == DBNull.Value ? null : dataTable.Rows[i][7].ToString().Trim();
                                if (string.IsNullOrWhiteSpace(unitOfMeasure))
                                {
                                    changedRecord.OldUnitOfMeasure = unitOfMeasure;
                                    changedRecord.NewUnitOfMeasure = "1";
                                    unitOfMeasure = "1";
                                }
                                command.Parameters.AddWithValue("@unitofmeasure", Convert.ToInt32(unitOfMeasure));

                                command.Parameters.AddWithValue("@priority", dataTable.Rows[i][8] == DBNull.Value ? 0 : Convert.ToInt32(dataTable.Rows[i][8]));

                                command.Parameters.AddWithValue("@stock", 0); // dataTable.Rows[i][9] == DBNull.Value ? 0 : Convert.ToInt32(dataTable.Rows[i][9]));

                                command.Parameters.AddWithValue("@issue", 0); // dataTable.Rows[i][10] == DBNull.Value ? 0 : Convert.ToInt32(dataTable.Rows[i][10]));

                                // since all the rate values have a first character as rupee sign, we can just go ahead and remove the first character and convert to float
                                //string rate = Convert.ToDouble(dataTable.Rows[i][11]);
                                //rate = rate.Trim();
                                //if (string.IsNullOrWhiteSpace(rate))
                                //    command.Parameters.AddWithValue("@rate", 0.00);
                                //else
                                //{
                                //    rate = rate.Substring(1);
                                command.Parameters.AddWithValue("@rate", dataTable.Rows[i][11] == DBNull.Value ? 0.00 : Convert.ToDouble(dataTable.Rows[i][11]));
                                //}

                                command.Parameters.AddWithValue("@moddate", dataTable.Rows[i][12] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][12])).ToString("yyyy-MM-dd hh:mm:ss"));

                                await command.ExecuteNonQueryAsync();

                                MedicineNo_MedicineId_Mapping[medicineNo] = guid;

                                if (!string.IsNullOrWhiteSpace(changedRecord.NewGroup) || !string.IsNullOrWhiteSpace(changedRecord.NewType) || !string.IsNullOrWhiteSpace(changedRecord.NewUnitOfMeasure))
                                {
                                    MedicinesWithChangedValues.Add(changedRecord);
                                }
                            }
                            catch (Exception ex)
                            {
                                MedicineRecordsWithExceptionsDict[Convert.ToString(dataTable.Rows[i][0])] = ex.Message;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortComplicationCategories()
        {
            // port data from CompliMaster to complication_categories
            /*
               CategoryId VARCHAR(36) NOT NULL,
              CategoryName VARCHAR(100) NULL,
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not NULL,
              CategoryPriority varchar(36) not null,
             */

            //

            GetAccessData("select distinct(department) from CompliMaster;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into complications_category (CategoryId,CategoryName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId,CategoryPriority) values";
                        int priority = 0;
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            string categoryId = string.Empty;
                            string categoryName = dataTable.Rows[i][0].ToString().Trim();
                            if (ComplicationCategories.Keys.Contains(categoryName.ToUpper()))
                                categoryId = ComplicationCategories[categoryName.ToUpper()];
                            else
                            {
                                NewlyAddedComplicationCategories.Add(categoryName);
                                ComplicationCategories[categoryName.ToUpper()] = categoryId = Guid.NewGuid().ToString();
                            }

                            command.CommandText += $"('{categoryId}', '{categoryName}', {locationId}, '{defaultCreatedDate}', null,null,'{adminUserId}',{++priority})";
                            if (i != dataTable.Rows.Count - 1)
                                command.CommandText += ',';
                        }

                        int x = await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSymptomCategories()
        {
            // port data from symptomsmaster to symptoms_category
            /*
               CategoryId VARCHAR(36) NOT NULL,
              CategoryName VARCHAR(100) NULL,
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not NULL,
              CategoryPriority varchar(36) not null,
             */

            //

            GetAccessData("select distinct(department) from symptomsmaster;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into symptoms_category (CategoryId,CategoryName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId,CategoryPriority) values";
                        int priority = 0;
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            string categoryId = string.Empty;
                            string categoryName = dataTable.Rows[i][0].ToString().Trim();
                            if (SymptomCategories.Keys.Contains(categoryName.ToUpper()))
                                categoryId = SymptomCategories[categoryName.ToUpper()];
                            else
                            {
                                NewlyAddedSymptomCategories.Add(categoryName);
                                SymptomCategories[categoryName.ToUpper()] = categoryId = Guid.NewGuid().ToString();
                            }

                            command.CommandText += $"('{categoryId}', '{categoryName}', {locationId}, '{defaultCreatedDate}', null,null,'{adminUserId}',{++priority})";
                            if (i != dataTable.Rows.Count - 1)
                                command.CommandText += ',';
                        }

                        int x = await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSignCategories()
        {
            // port data from signmaster to signs_category
            /*
              CategoryId VARCHAR(36) NOT NULL,
              CategoryName VARCHAR(100) NULL,
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not NULL,
              CategoryPriority varchar(36) not null,
             */

            //

            GetAccessData("select distinct(department) from SignMaster;");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into signs_category (CategoryId,CategoryName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId,CategoryPriority) values";
                        int priority = 0;
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            string categoryId = string.Empty;
                            string categoryName = dataTable.Rows[i][0].ToString().Trim();
                            if (SignCategories.Keys.Contains(categoryName.ToUpper()))
                                categoryId = SignCategories[categoryName.ToUpper()];
                            else
                            {
                                NewlyAddedSignCategories.Add(categoryName);
                                SignCategories[categoryName.ToUpper()] = categoryId = Guid.NewGuid().ToString();
                            }

                            command.CommandText += $"('{categoryId}', '{categoryName}', {locationId}, '{defaultCreatedDate}', null,null,'{adminUserId}',{++priority})";
                            if (i != dataTable.Rows.Count - 1)
                                command.CommandText += ',';
                        }

                        int x = await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortTestCategories()
        {
            // port data from InvestigationMaster to test_category
            /*
              CategoryId VARCHAR(36) NOT NULL,
              CategoryName VARCHAR(100) NULL,
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not NULL,
             */

            //

            GetAccessData("select distinct(department) from InvestigationMaster;");

            // Note - For test categories, we must also add GENERAL test category. This is not in investigationmaster in accessDB, but we need it. It has been earlier added to TestCategories
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = "insert into test_category (CategoryId,CategoryName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                        bool hasGeneralBeenAdded = false;
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            string categoryId = string.Empty;
                            string categoryName = dataTable.Rows[i][0].ToString().Trim();

                            if (categoryName.ToUpper() == "GENERAL")
                                hasGeneralBeenAdded = true;

                            if (TestCategories.Keys.Contains(categoryName.ToUpper()))
                                categoryId = TestCategories[categoryName.ToUpper()];
                            else
                            {
                                NewlyAddedTestCategories.Add(categoryName);
                                TestCategories[categoryName.ToUpper()] = categoryId = Guid.NewGuid().ToString();
                            }

                            command.CommandText += $"('{categoryId}', '{categoryName}', {locationId}, '{defaultCreatedDate}', null,null,'{adminUserId}')";
                            if (i != dataTable.Rows.Count - 1)
                                command.CommandText += ',';
                        }

                        if(!hasGeneralBeenAdded)
                        {
                            string categoryId = TestCategories["GENERAL"];
                            command.CommandText += $",('{categoryId}', 'GENERAL', {locationId}, '{defaultCreatedDate}', null,null,'{adminUserId}')";
                        }

                        int x = await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortPrescriptions()
        {
            // Prescriptions have been ported from the Treatment table in accessDB
            /*  PrescriptionId          !guid!
                MedicineId              ! taken from dict, mapped to iMedicineNo!
                PatientId               numPatientId
                Dosage                  cDose
                ExtraInfo               mdays
                ReasonToChange          reason_to_change
                DtDate                  dtDate
                LocationId
                CreatedDate
                UpdatedDate
                DeletedDate
                UserId
            */

            // We will get the medicineid from the dict. and verify if patient exists in table in PatientIds 
            List<int> patientIds = new List<int>();
            try
            {
                GetAccessData("select distinct(numPatientId) from Treatment where numPatientId is not null order by numPatientId");
                for (int i = 0; i < dataTable.Rows.Count; i++)
                {
                    patientIds.Add(Convert.ToInt32(dataTable.Rows[i][0]));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception while reading patientids: {ex.Message}");
            }

            foreach (var patId in patientIds)
            {
                try
                {
                    GetAccessData($"select iMedicineNo, numPatientId,dtDate, cDose, mdays, reason_to_change,Mod_Date from Treatment where numPatientId = {patId} order by dtDate,iMedicineNo, Mod_Date desc;");
                    int totalRecordsInserted = 0;
                    using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                    {
                        using (MySqlCommand command = new MySqlCommand())
                        {
                            await connection.OpenAsync();
                            command.Connection = connection;

                            int insertRecordsInBatchesOf = 5000;
                            int i = 0;
                            int j = 0;
                            bool doesCommandTextHaveRows = false;
                            string savedmedIdDtDate = string.Empty;
                            DateTime? savedmoddate = null;

                            while (i < dataTable.Rows.Count)
                            {
                                command.CommandText = "insert into prescriptions(PrescriptionId,MedicineId,PatientId,Dosage,ExtraInfo,ReasonToChange,DtDate,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";

                                for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                                {
                                    try
                                    {
                                        // command.Parameters.Clear();
                                        doesCommandTextHaveRows = false;
                                        int medicineNo = Convert.ToInt32(dataTable.Rows[i][0]);
                                        string medicineId = string.Empty;
                                        if (MedicineNo_MedicineId_Mapping.Keys.Contains(medicineNo))
                                            medicineId = MedicineNo_MedicineId_Mapping[medicineNo];
                                        else
                                            throw new Exception($"Medicine does not exist in medicines table");

                                        int patientId = Convert.ToInt32(dataTable.Rows[i][1]);
                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception($"Patient Id does not exist in patients table");

                                        string dtdate = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        if (patientId == 808 && medicineNo == 326 && dtdate.Equals("2018-03-13 12:00:00"))
                                        {

                                        }


                                        string dosage = dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim());

                                        string extrainfo = dataTable.Rows[i][4] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][4].ToString().Trim());

                                        string reasontochange = dataTable.Rows[i][5] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][5].ToString().Trim());

                                        string moddate = dataTable.Rows[i][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string deleteddate = null;
                                        // If the medId/dtdate exists in savedmedid check if moddate is lesser than savedmoddate. 
                                        // if yes then set deleteddate = moddate and save + add to list 
                                        // not doing this for now ==> // else set deleted date of existing record -> add that to list and insert this 
                                        string newmedIdDtDate = $"{medicineId}/{dtdate}";
                                        if (savedmedIdDtDate.Equals(newmedIdDtDate))
                                        {
                                            deleteddate = defaultCreatedDate;
                                            DuplicatePrescriptionsSetWithDeletedDate.Add($"PatientId: {patientId} ## MedicineNo: {medicineNo} ## DtDate: {dtdate} ## ModDate: {moddate} < SavedModDate: {savedmoddate?.ToString("yyyy-MM-dd hh:mm:ss")} ## MedicineId:{medicineId}");
                                        }
                                        else
                                        {
                                            savedmedIdDtDate = newmedIdDtDate;
                                            savedmoddate = dataTable.Rows[i][6] == DBNull.Value ? null : (DateTime?)(Convert.ToDateTime(dataTable.Rows[i][6]));
                                        }

                                        command.CommandText += $"('{Guid.NewGuid().ToString()}','{medicineId }',{patientId},'{dosage}','{extrainfo}','{reasontochange}','{dtdate}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null," : $"'{moddate}',";
                                        command.CommandText += deleteddate == null ? $"null" : $"'{deleteddate}'";
                                        command.CommandText += $",'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;

                                    }
                                    catch (Exception ex)
                                    {
                                        string key = $"{dataTable.Rows[i][0].ToString().Trim()}/{dataTable.Rows[i][1].ToString().Trim()}/{(Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss")}";
                                        if(PrescriptionRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{dataTable.Rows[i][0].ToString().Trim()}/{dataTable.Rows[i][1].ToString().Trim()}/{(Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss")}/{PrescriptionRecordsWithExceptionsDict.Count}";
                                        PrescriptionRecordsWithExceptionsDict[key] = ex.Message;
                                    }

                                    if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                    {
                                        if (command.CommandText.EndsWith(","))
                                            command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                        command.CommandText += ";";
                                    }

                                }
                                try
                                {
                                    if (!doesCommandTextHaveRows)
                                        continue;
                                    int x = await command.ExecuteNonQueryAsync();
                                    totalRecordsInserted += x;
                                    Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{PrescriptionRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                                }
                                catch (Exception ex)
                                {
                                    // the batch of prescriptions could not be inserted successfully. 
                                    // so insert individually 
                                    Console.WriteLine($"In retry: {ex.Message} Inner: {ex.InnerException?.Message}");

                                    int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                    for (int count = 1; count <= j; retryCount++, count++)
                                    {
                                        try
                                        {
                                            command.Parameters.Clear();

                                            int medicineNo = Convert.ToInt32(dataTable.Rows[retryCount][0]);
                                            string medicineId = string.Empty;
                                            if (MedicineNo_MedicineId_Mapping.Keys.Contains(medicineNo))
                                                medicineId = MedicineNo_MedicineId_Mapping[medicineNo];
                                            else
                                                throw new Exception($"Medicine does not exist in medicines table");

                                            command.Parameters.AddWithValue("@medicineid", medicineId);

                                            int patientId = Convert.ToInt32(dataTable.Rows[retryCount][1]);
                                            if (PatientIds.Contains(patientId))
                                                command.Parameters.AddWithValue("@patientid", patientId);
                                            else
                                                throw new Exception($"Patient Id does not exist in patients table");


                                            string dosage = dataTable.Rows[retryCount][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][3].ToString().Trim());

                                            string extrainfo = dataTable.Rows[retryCount][4] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][4].ToString().Trim());

                                            string reasontochange= dataTable.Rows[retryCount][5] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][5].ToString().Trim());

                                            string dtdate = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                            string moddate= dataTable.Rows[retryCount][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                            string deleteddate = null;
                                            
                                            // If the medId/dtdate exists in savedmedid check if moddate is lesser than savedmoddate. 
                                            // if yes then set deleteddate = moddate and save + add to list 
                                            // not doing this for now ==> // else set deleted date of existing record -> add that to list and insert this 
                                            string newmedIdDtDate = $"{medicineId}/{dtdate}";
                                            if (savedmedIdDtDate.Equals(newmedIdDtDate))
                                            {
                                                deleteddate = moddate;
                                                DuplicatePrescriptionsSetWithDeletedDate.Add($"PatientId: {patientId} ## MedicineNo: {medicineNo} ## DtDate: {dtdate} ## ModDate: {moddate} < SavedDate: {savedmoddate?.ToString("yyyy - MM - dd hh: mm:ss")} ## MedicineId:{medicineId}");
                                            }
                                            else
                                            {
                                                savedmedIdDtDate = newmedIdDtDate;
                                                savedmoddate = dataTable.Rows[retryCount][6] == DBNull.Value ? null : (DateTime?)(Convert.ToDateTime(dataTable.Rows[retryCount][6]));
                                            }

                                            command.CommandText = "insert into prescriptions(PrescriptionId,MedicineId,PatientId,Dosage,ExtraInfo,ReasonToChange,DtDate,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                                         $"('{Guid.NewGuid().ToString()}','{medicineId}',{patientId},'{dosage}','{extrainfo}','{reasontochange}','{dtdate}',{locationId},'{defaultCreatedDate}',";
                                            command.CommandText += moddate == null ? "null," : $"'{moddate}',";
                                            command.CommandText += deleteddate == null ? "null" : $"'{deleteddate}'";
                                            command.CommandText += $"'{adminUserId}');";

                                            int x = await command.ExecuteNonQueryAsync();
                                            totalRecordsInserted += x;
                                            Console.WriteLine($"PatientId: {patientId} ## Records inserted:{x} ### ExceptionRecords:{PrescriptionRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                        }
                                        catch (Exception individualex)
                                        {
                                            string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{dataTable.Rows[retryCount][1].ToString().Trim()}/{(Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss")}";
                                            if (PrescriptionRecordsWithExceptionsDict.Keys.Contains(key))
                                                key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{dataTable.Rows[retryCount][1].ToString().Trim()}/{(Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss")}/{PrescriptionRecordsWithExceptionsDict.Count}";
                                            PrescriptionRecordsWithExceptionsDict[key] = individualex.Message;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                    Console.WriteLine($"Total exception records: {PrescriptionRecordsWithExceptionsDict.Count}");
                    Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.ReadLine();
                    string key = $"All med nos / {patId}/ all dtdates";
                    PrescriptionRecordsWithExceptionsDict[key]= ex.Message;
                }
            }
        }

        public async Task CheckPrescriptionsValidity()
        {
            int total = 0;
            for (int i = 0; i < PatientIds.Count; i++)
            {
                int patientId = PatientIds[i];
                GetAccessData($"select count(*) from Treatment where numPatientId = {patientId};");
                int accessDBCount = Convert.ToInt32(dataTable.Rows[0][0]);
                int mysqlCount = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = $"select count(*) as count from prescriptions where PatientId = {patientId}";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                mysqlCount = Convert.ToInt32(reader["count"]);
                            }
                        }
                    }
                }

                if (accessDBCount != mysqlCount)
                {
                    total += (mysqlCount - accessDBCount);
                    Console.WriteLine($"PatientId: {patientId}  ### AccessDBCount: {accessDBCount} ### MysqlCount: {mysqlCount} ### diff {mysqlCount - accessDBCount}");
                }
            }
            Console.WriteLine($"total: {total}");
        }

        public async Task PortPrescriptionComments()
        {
            // We take the prescription comments from the Treatment table in accessdb

            /*   CommentId varchar(36) not null primary key,         
                 PatientId int(10) unsigned not null,                numPatientId
                 DtDate datetime not null,                           dtdate
                 Comments text,                                      comments
                 LocationId int(3) unsigned not null,
                 CreatedDate datetime not null,
                 UpdatedDate datetime,                               Mod_Date
                 DeletedDate datetime,
                 UserId varchar(36)
             */

            GetAccessData("select numPatientId, dtDate, comments, Mod_Date from Treatment where comments is not null and not comments = ' ' order by numPatientId, dtDate ");

            using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
            {
                using (MySqlCommand command = new MySqlCommand())
                {
                    await connection.OpenAsync();
                    command.Connection = connection;

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        try
                        {
                            command.Parameters.Clear();
                            command.CommandText = $"insert into prescriptioncomments (CommentId,PatientId,DtDate,Comments,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values ('{Guid.NewGuid().ToString()}', @patientid, @dtdate, @comments, {locationId}, '{defaultCreatedDate}', @updateddate, null, '{adminUserId}');";
                            command.Parameters.AddWithValue("@patientid", dataTable.Rows[i][0]);
                            command.Parameters.AddWithValue("@dtdate", dataTable.Rows[i][1] == DBNull.Value ? null : Convert.ToDateTime(dataTable.Rows[i][1]).ToString("yyyy-MM-dd hh:mm:ss"));
                            string comments = dataTable.Rows[i][2].ToString().Trim();
                            string temp = comments.Replace("\r\n", "");
                            if (string.IsNullOrWhiteSpace(temp))
                            {
                                comments = comments.Replace("\r\n", "\\r\\n");
                                throw new Exception($"The comment only contains \\r\\n, hence not porting. Comment: {comments}");
                            }
                            command.Parameters.AddWithValue("@comments", MySqlHelper.EscapeString(Convert.ToString(dataTable.Rows[i][2]).Trim()));
                            command.Parameters.AddWithValue("@updateddate", dataTable.Rows[i][3] == DBNull.Value ? null : Convert.ToDateTime(dataTable.Rows[i][3]).ToString("yyyy-MM-dd hh:mm:ss"));

                            int x = await command.ExecuteNonQueryAsync();
                        }
                        catch (Exception ex)
                        {
                            string key = $"{dataTable.Rows[i][0]} / {dataTable.Rows[i][1]} ";
                            PrescriptionCommentRecordsWithExceptionsDict[key] = ex.Message;
                        }
                    }
                }
            }
        }

        public async Task PortEvents()
        {
            // port data from Events to events
            /*
                EventId varchar(36) primary key,
                PatientId int(7) unsigned not null,     numPatientId
                DateOfEvent datetime,                   DOE
                EventName varchar(100),                 Event
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,          strDay, strMonth, strYear -- if they are blank then default createddate
                UpdatedDate datetime ,                  Mod_Date
                DeletedDate datetime,
                UserId varchar(36) 
             */

            GetAccessData("select numPatientId, DOE, Event, strDay, strMonth, strYear, Mod_Date from Events order by numPatientId, DOE;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 5000;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into events (EventId,PatientId,DateOfEvent,EventName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";

                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    int patientId = Convert.ToInt32(dataTable.Rows[i][0]);
                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception($"Patient Id does not exist in patients table");

                                    string dtdate = dataTable.Rows[i][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                    if (dtdate == null)
                                        throw new Exception($"Date of Event cannot be null");

                                    string eventname = dataTable.Rows[i][2] ==null? null: MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());
                                    if (string.IsNullOrWhiteSpace(eventname))
                                        throw new Exception($"Event name cannot be null");

                                    string day = Convert.ToString(dataTable.Rows[i][3]);
                                    string month = Convert.ToString(dataTable.Rows[i][4]);
                                    string year = Convert.ToString(dataTable.Rows[i][5]);
                                    string createdDate = defaultCreatedDate;
                                    if (!string.IsNullOrWhiteSpace(day) && !string.IsNullOrWhiteSpace(month) && !string.IsNullOrWhiteSpace(year))
                                    {
                                        createdDate = $"{year}-{month}-{day} 00:00:00";
                                    }

                                    string moddate = dataTable.Rows[i][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{dtdate}','{eventname}',{locationId},'{createdDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    string patientid = dataTable.Rows[i][0].ToString();
                                    string eventdate = dataTable.Rows[i][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                    string name = dataTable.Rows[i][2] == null? null: dataTable.Rows[i][2].ToString().Trim();
                                    string key = $"{patientid}/{eventdate}/{name}";
                                    if(EventRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{patientid}/{eventdate}/{name}/{EventRecordsWithExceptionsDict.Keys.Count}"; 
                                    // adding a random no. to the key so that a record with the same key does not get overwritten. 
                                    // Earlier, due to the overwrite we were getting less no. of records in the file then expected as same key would result in overwrite.
                                    EventRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{EventRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        command.Parameters.Clear();


                                        int patientId = Convert.ToInt32(dataTable.Rows[retryCount][0]);
                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception($"Patient Id does not exist in patients table");

                                        string dtdate = dataTable.Rows[retryCount][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                        if (dtdate == null)
                                            throw new Exception($"Date of Event cannot be null");

                                        string eventname = dataTable.Rows[retryCount][2] == null? null: MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());
                                        if (string.IsNullOrWhiteSpace(eventname))
                                            throw new Exception($"Event name cannot be null");

                                        string day = Convert.ToString(dataTable.Rows[retryCount][3]);
                                        string month = Convert.ToString(dataTable.Rows[retryCount][4]);
                                        string year = Convert.ToString(dataTable.Rows[retryCount][5]);
                                        string createdDate = defaultCreatedDate;
                                        if (!string.IsNullOrWhiteSpace(day) && !string.IsNullOrWhiteSpace(month) && !string.IsNullOrWhiteSpace(year))
                                        {
                                            createdDate = $"{year}-{month}-{day} 00:00:00";
                                        }

                                        string moddate = dataTable.Rows[retryCount][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][6])).ToString("yyyy-MM-dd hh:mm:ss");



                                        command.CommandText = "insert into events (EventId,PatientId,DateOfEvent,EventName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                                         $"('{Guid.NewGuid().ToString()}',{patientId},'{dtdate}','{eventname}',{locationId},'{createdDate}','{moddate}',null,'{adminUserId}');";

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{EventRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string patientid = dataTable.Rows[retryCount][0].ToString().Trim();
                                        string eventdate = dataTable.Rows[retryCount][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                        string name = dataTable.Rows[retryCount][2] == null? null: dataTable.Rows[retryCount][2].ToString().Trim();
                                        string key = $"{patientid}/{eventdate}/{name}";
                                        if (EventRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{patientid}/{eventdate}/{name}/{EventRecordsWithExceptionsDict.Keys.Count}";
                                        // adding a random no. to the key so that a record with the same key does not get overwritten. 
                                        // Earlier, due to the overwrite we were getting less no. of records in the file then expected as same key would result in overwrite.
                                        EventRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {EventRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortIllnesses()
        {
            // port data from Illness to illness
            /*
                IllnessId varchar(36) primary key,      
                PatientId int(7) unsigned not null,     numpatientid
                DateOfEntry datetime,                   DOE
                IllnessOrOperationName varchar(100),    illness
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,          strDay, strMonth, strYear 
                UpdatedDate datetime ,                  Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
             */

            GetAccessData("select numPatientId, DOE, illness, strDay, strMonth, strYear, Mod_Date from Illness order by numPatientId, DOE;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 5000;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into illness (IllnessId,PatientId,DateOfEntry,IllnessOrOperationName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";

                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    int patientId = Convert.ToInt32(dataTable.Rows[i][0]);
                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception($"Patient Id does not exist in patients table");

                                    string dtdate = dataTable.Rows[i][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                    if (dtdate == null)
                                        throw new Exception($"Date of Event cannot be null");

                                    string illnessName = MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());
                                    if (string.IsNullOrWhiteSpace(illnessName))
                                        throw new Exception($"Illness name cannot be null");

                                    string day = Convert.ToString(dataTable.Rows[i][3]);
                                    string month = Convert.ToString(dataTable.Rows[i][4]);
                                    string year = Convert.ToString(dataTable.Rows[i][5]);
                                    string createdDate = defaultCreatedDate;
                                    if (!string.IsNullOrWhiteSpace(day) && !string.IsNullOrWhiteSpace(month) && !string.IsNullOrWhiteSpace(year))
                                    {
                                        createdDate = $"{year}-{month}-{day} 00:00:00";
                                    }

                                    string moddate = dataTable.Rows[i][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{dtdate}','{illnessName}',{locationId},'{createdDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    string patientid = dataTable.Rows[i][0].ToString().Trim();
                                    string illnessDate = dataTable.Rows[i][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                    string name = dataTable.Rows[i][2] == null? null: dataTable.Rows[i][2].ToString().Trim();
                                    string key = $"{patientid}/{illnessDate}/{name}";
                                    if (IllnessRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{patientid}/{illnessDate}/{name}/{IllnessRecordsWithExceptionsDict.Keys.Count}";
                                    IllnessRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{IllnessRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        command.Parameters.Clear();


                                        int patientId = Convert.ToInt32(dataTable.Rows[retryCount][0]);
                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception($"Patient Id does not exist in patients table");

                                        string dtdate = dataTable.Rows[retryCount][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                        if (dtdate == null)
                                            throw new Exception($"Date of Event cannot be null");

                                        string illnessName = MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());
                                        if (string.IsNullOrWhiteSpace(illnessName))
                                            throw new Exception($"Illness name cannot be null");

                                        string day = Convert.ToString(dataTable.Rows[retryCount][3]);
                                        string month = Convert.ToString(dataTable.Rows[retryCount][4]);
                                        string year = Convert.ToString(dataTable.Rows[retryCount][5]);
                                        string createdDate = defaultCreatedDate;
                                        if (!string.IsNullOrWhiteSpace(day) && !string.IsNullOrWhiteSpace(month) && !string.IsNullOrWhiteSpace(year))
                                        {
                                            createdDate = $"{year}-{month}-{day} 00:00:00";
                                        }

                                        string moddate = dataTable.Rows[retryCount][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][6])).ToString("yyyy-MM-dd hh:mm:ss");



                                        command.CommandText = "insert into illness (IllnessId,PatientId,DateOfEntry,IllnessOrOperationName,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values" +
                                                         $"('{Guid.NewGuid().ToString()}',{patientId},'{dtdate}','{illnessName}',{locationId},'{createdDate}','{moddate}',null,'{adminUserId}');";

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{IllnessRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string patientid = dataTable.Rows[retryCount][0].ToString().Trim();
                                        string illnessDate = dataTable.Rows[retryCount][1] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][1])).ToString("yyyy-MM-dd hh:mm:ss");
                                        string illness = dataTable.Rows[retryCount][2].ToString().Trim();
                                        string key = $"{patientid}/{illnessDate}/{illness}";
                                        if (IllnessRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{patientid}/{illnessDate}/{illness}/{IllnessRecordsWithExceptionsDict.Keys.Count}";
                                        IllnessRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {IllnessRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortForms()
        {
            // port from forms to forms 
            /*
            PatientFormId  varchar(36) primary key not null,
            FormFormatId varchar(36) not null,                  form_name -- get the id from formformats dict
            PatientId int(7) unsigned not null,                 numpatientid
            Form mediumtext not null,                           form_format
            FormDate datetime not null,                         dtformdate
            CreatedDate datetime not null,
            UpdatedDate datetime,                               Mod_Date
            DeletedDate datetime,
            LocationId int(3) unsigned not null,
            UserId varchar(36) not null
             */

            GetAccessData("select form_name,numpatientid,form_format, dtformdate,Mod_Date from forms order by numPatientid;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 500;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        int patientId = 0;
                        string formFormatName = string.Empty;
                        string formdate = string.Empty;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into forms (PatientFormId,FormFormatId,PatientId,Form,FormDate,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    formFormatName = MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim());
                                    string formFormatId = string.Empty;
                                    if (FormFormats.Keys.Contains(formFormatName.ToUpper()))
                                        formFormatId = FormFormats[formFormatName.ToUpper()];
                                    else
                                        throw new Exception("FormFormatName does not exist in FormsMaster table");

                                    patientId = Convert.ToInt32(dataTable.Rows[i][1]);
                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception("PatientId does not exist in DB");

                                    string form = dataTable.Rows[i][2] == DBNull.Value ? null : MySqlHelper.EscapeString((Convert.ToString(dataTable.Rows[i][2])));

                                    formdate = dataTable.Rows[i][3] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][3])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string moddate = dataTable.Rows[i][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}','{formFormatId}',{patientId},'{form}','{formdate}',{locationId},'{defaultCreatedDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    formdate = dataTable.Rows[i][3] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][3])).ToString("yyyy-MM-dd hh:mm:ss");
                                    string key = $"{formFormatName}/{Convert.ToUInt32(dataTable.Rows[i][1])}/{formdate}";
                                    if (InvalidForms.Keys.Contains(key))
                                        key = $"{formFormatName}/{Convert.ToUInt32(dataTable.Rows[i][1])}/{formdate}/{InvalidForms.Keys.Count}";
                                    InvalidForms[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }
                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidForms.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        formFormatName = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim());
                                        string formFormatId = string.Empty;
                                        if (FormFormats.Keys.Contains(formFormatName.ToUpper()))
                                            formFormatId = FormFormats[formFormatName.ToUpper()];
                                        else
                                            throw new Exception("FormFormatName does not exist in FormsMaster table");

                                        patientId = Convert.ToInt32(dataTable.Rows[retryCount][1]);
                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        string form = dataTable.Rows[retryCount][2] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString());

                                        formdate = dataTable.Rows[retryCount][3] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][3])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string moddate = dataTable.Rows[retryCount][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText = "insert into forms (PatientFormId,FormFormatId,PatientId,Form,FormDate,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        command.CommandText += $"('{Guid.NewGuid().ToString()}','{formFormatId}',{patientId},'{form}','{formdate}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidForms.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        formdate = dataTable.Rows[retryCount][3] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][3])).ToString("yyyy-MM-dd hh:mm:ss");
                                        string key = $"{formFormatName}/{Convert.ToUInt32(dataTable.Rows[retryCount][1])}/{formdate}";
                                        if (InvalidForms.Keys.Contains(key))
                                            key = $"{formFormatName}/{Convert.ToUInt32(dataTable.Rows[retryCount][1])}/{formdate}/{InvalidForms.Keys.Count}";
                                        InvalidForms[key] = individualex.Message;
                                    }
                                }
                            }
                        }
                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvalidForms.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortComplicationMaster()
        {
            // port data from CompliMaster to complicationmaster
            /*
                ComplicationMasterId varchar(36) not null primary key,   
                ComplicationCode varchar(36) not null,                      strCode 
                Name varchar(100) not null,                                 strName
                NameDefault varchar(100)                                    str_default
                Min int(3),
                Max int(3),
                Priority int(3),                                            numPriority
                 F1 varchar(100),                                           f1
                 F2 varchar(100),                                           f2
                 F3 varchar(100),                                           f3
                 F4 varchar(100),                                           f4
                 F5 varchar(100),                                           f5
                 F6 varchar(100),                                           f6
                 F7 varchar(100),                                           f7
                 F8 varchar(100),                                           f8
                 F9 varchar(100),                                           f9
                 F10 varchar(100),                                          f10
                 F11 varchar(100),                                          f11
                F12 varchar(100),                                           f12
                ComplicationCategoryId varchar(36) not null,                department
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                                       Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
             */

            GetAccessData("select strCode, strName, str_default, numPriority,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12, Mod_Date, department from CompliMaster;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 2;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        List<string> insertedCompCodes = new List<string>();
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into complicationmaster (ComplicationMasterId,ComplicationCode,Name,NameDefault,Priority,ComplicationCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            List<string> tempCompCodes = new List<string>();
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    string complicationCode = MySqlHelper.EscapeString(dataTable.Rows[i][0] == null ? null : dataTable.Rows[i][0].ToString().Trim());
                                    if (complicationCode == null || insertedCompCodes.Contains(complicationCode) || tempCompCodes.Contains(complicationCode))
                                        throw new Exception("Invalid ComplicationCode. Either null or duplicate");

                                    string name = MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());

                                    string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());

                                    int priority = Convert.ToInt32(dataTable.Rows[i][3]);

                                    string F1 = dataTable.Rows[i][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][4].ToString().Trim());
                                    string F2 = dataTable.Rows[i][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][5].ToString().Trim());
                                    string F3 = dataTable.Rows[i][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][6].ToString().Trim());
                                    string F4 = dataTable.Rows[i][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][7].ToString().Trim());
                                    string F5 = dataTable.Rows[i][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][8].ToString().Trim());
                                    string F6 = dataTable.Rows[i][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][9].ToString().Trim());
                                    string F7 = dataTable.Rows[i][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][10].ToString().Trim());
                                    string F8 = dataTable.Rows[i][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][11].ToString().Trim());
                                    string F9 = dataTable.Rows[i][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][12].ToString().Trim());
                                    string F10 = dataTable.Rows[i][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][13].ToString().Trim());
                                    string F11 = dataTable.Rows[i][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][14].ToString().Trim());
                                    string F12 = dataTable.Rows[i][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][15].ToString().Trim());

                                    string moddate = dataTable.Rows[i][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string complication_category_id = string.Empty;
                                    string categoryName = dataTable.Rows[i][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][17].ToString().Trim());
                                    if (categoryName == null || !ComplicationCategories.Keys.Contains(categoryName.ToUpper()))
                                        throw new Exception("Row does not have valid category");
                                    else
                                        complication_category_id = ComplicationCategories[categoryName.ToUpper()];

                                    ComplicationCodes[complicationCode.ToUpper()] = Guid.NewGuid().ToString();
                                    command.CommandText += $"('{ComplicationCodes[complicationCode.ToUpper()]}','{complicationCode}','{name}','{nameDefault}',{priority},'{complication_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                    tempCompCodes.Add(complicationCode);
                                }
                                catch (Exception ex)
                                {
                                    string key = $"{dataTable.Rows[i][0].ToString().Trim()}";
                                    if (ComplicationMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{dataTable.Rows[i][0].ToString().Trim()}/{ComplicationMasterRecordsWithExceptionsDict.Keys.Count}";
                                    ComplicationMasterRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                insertedCompCodes.AddRange(tempCompCodes);
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{ComplicationMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 
                                tempCompCodes.Clear();
                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        string complicationCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0] == null ? null : dataTable.Rows[retryCount][0].ToString().Trim());
                                        if (complicationCode == null || insertedCompCodes.Contains(complicationCode))
                                            throw new Exception("Invalid ComplicationCode. Either null or duplicate");

                                        string name = MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());

                                        string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());

                                        int priority = Convert.ToInt32(dataTable.Rows[retryCount][3]);

                                        string F1 = dataTable.Rows[retryCount][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][4].ToString().Trim());
                                        string F2 = dataTable.Rows[retryCount][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][5].ToString().Trim());
                                        string F3 = dataTable.Rows[retryCount][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][6].ToString().Trim());
                                        string F4 = dataTable.Rows[retryCount][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][7].ToString().Trim());
                                        string F5 = dataTable.Rows[retryCount][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][8].ToString().Trim());
                                        string F6 = dataTable.Rows[retryCount][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][9].ToString().Trim());
                                        string F7 = dataTable.Rows[retryCount][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][10].ToString().Trim());
                                        string F8 = dataTable.Rows[retryCount][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][11].ToString().Trim());
                                        string F9 = dataTable.Rows[retryCount][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][12].ToString().Trim());
                                        string F10 = dataTable.Rows[retryCount][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][13].ToString().Trim());
                                        string F11 = dataTable.Rows[retryCount][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][14].ToString().Trim());
                                        string F12 = dataTable.Rows[retryCount][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][15].ToString().Trim());

                                        string moddate = dataTable.Rows[retryCount][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string complication_category_id = string.Empty;
                                        string categoryName = dataTable.Rows[retryCount][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][17].ToString().Trim());
                                        if (categoryName == null || !ComplicationCategories.Keys.Contains(categoryName.ToUpper()))
                                            throw new Exception("Row does not have valid category");
                                        else
                                            complication_category_id = ComplicationCategories[categoryName.ToUpper()];

                                        ComplicationCodes[complicationCode.ToUpper()] = Guid.NewGuid().ToString();
                                        command.CommandText = "insert into complicationmaster (ComplicationMasterId,ComplicationCode,Name,NameDefault,Priority,ComplicationCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        //  $"('{Guid.NewGuid().ToString()}','{complicationCode}','{name}','{nameDefault}',{priority},'{complication_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}','{moddate}',null,'{adminUserId}')";
                                        command.CommandText += $"('{ComplicationCodes[complicationCode.ToUpper()]}','{complicationCode}','{name}','{nameDefault}',{priority},'{complication_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";

                                        int x = await command.ExecuteNonQueryAsync();
                                        insertedCompCodes.Add(complicationCode);
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{ComplicationMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}";
                                        if (ComplicationMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{ComplicationMasterRecordsWithExceptionsDict.Keys.Count}";
                                        ComplicationMasterRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {ComplicationMasterRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSymptomsMaster()
        {
            // port data from symptomsmaster to symptomsmaster
            /*
                SymptomMasterId varchar(36) not null primary key,   
                SymptomCode varchar(36) not null,                           strCode 
                Name varchar(100) not null,                                 strName
                NameDefault varchar(100)                                    str_default
                Min int(3),
                Max int(3),
                Priority int(3),                                            numPriority
                 F1 varchar(100),                                           f1
                 F2 varchar(100),                                           f2
                 F3 varchar(100),                                           f3
                 F4 varchar(100),                                           f4
                 F5 varchar(100),                                           f5
                 F6 varchar(100),                                           f6
                 F7 varchar(100),                                           f7
                 F8 varchar(100),                                           f8
                 F9 varchar(100),                                           f9
                 F10 varchar(100),                                          f10
                 F11 varchar(100),                                          f11
                F12 varchar(100),                                           f12
                SymptomCategoryId varchar(36) not null,                     department
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                                       Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
             */

            GetAccessData("select strCode, strName, str_default, numPriority,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12, Mod_Date, department from symptomsmaster;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 2;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        List<string> insertedSymptomCodes = new List<string>();
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into symptomsmaster (SymptomMasterId,SymptomCode,Name,NameDefault,Priority,SymptomCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            List<string> tempCompCodes = new List<string>();
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    string symptomCode = MySqlHelper.EscapeString(dataTable.Rows[i][0] == null ? null : dataTable.Rows[i][0].ToString().Trim());
                                    if (symptomCode == null || insertedSymptomCodes.Contains(symptomCode) || tempCompCodes.Contains(symptomCode))
                                        throw new Exception("Invalid ComplicationCode. Either null or duplicate");

                                    string name = MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());

                                    string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());

                                    int priority = Convert.ToInt32(dataTable.Rows[i][3]);

                                    string F1 = dataTable.Rows[i][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][4].ToString().Trim());
                                    string F2 = dataTable.Rows[i][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][5].ToString().Trim());
                                    string F3 = dataTable.Rows[i][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][6].ToString().Trim());
                                    string F4 = dataTable.Rows[i][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][7].ToString().Trim());
                                    string F5 = dataTable.Rows[i][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][8].ToString().Trim());
                                    string F6 = dataTable.Rows[i][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][9].ToString().Trim());
                                    string F7 = dataTable.Rows[i][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][10].ToString().Trim());
                                    string F8 = dataTable.Rows[i][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][11].ToString().Trim());
                                    string F9 = dataTable.Rows[i][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][12].ToString().Trim());
                                    string F10 = dataTable.Rows[i][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][13].ToString().Trim());
                                    string F11 = dataTable.Rows[i][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][14].ToString().Trim());
                                    string F12 = dataTable.Rows[i][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][15].ToString().Trim());

                                    string moddate = dataTable.Rows[i][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string symptom_category_id = string.Empty;
                                    string categoryName = dataTable.Rows[i][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][17].ToString().Trim());
                                    if (categoryName == null || !SymptomCategories.Keys.Contains(categoryName.ToUpper()))
                                        throw new Exception("Row does not have valid category");
                                    else
                                        symptom_category_id = SymptomCategories[categoryName.ToUpper()];
                                    SymptomCodes[symptomCode.ToUpper()] = Guid.NewGuid().ToString();
                                    command.CommandText += $"('{SymptomCodes[symptomCode.ToUpper()] }','{symptomCode}','{name}','{nameDefault}',{priority},'{symptom_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                    tempCompCodes.Add(symptomCode);
                                }
                                catch (Exception ex)
                                {
                                    string key = $"{dataTable.Rows[i][0].ToString().Trim()}";
                                    if(SymptomMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{dataTable.Rows[i][0].ToString().Trim()}/{SymptomMasterRecordsWithExceptionsDict.Keys.Count}";
                                    SymptomMasterRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                insertedSymptomCodes.AddRange(tempCompCodes);
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{SymptomMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 
                                tempCompCodes.Clear();
                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        string symptomCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0] == null ? null : dataTable.Rows[retryCount][0].ToString().Trim());
                                        if (symptomCode == null || insertedSymptomCodes.Contains(symptomCode))
                                            throw new Exception("Invalid SymptomCode. Either null or duplicate");

                                        string name = MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());

                                        string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());

                                        int priority = Convert.ToInt32(dataTable.Rows[retryCount][3]);

                                        string F1 = dataTable.Rows[retryCount][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][4].ToString().Trim());
                                        string F2 = dataTable.Rows[retryCount][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][5].ToString().Trim());
                                        string F3 = dataTable.Rows[retryCount][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][6].ToString().Trim());
                                        string F4 = dataTable.Rows[retryCount][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][7].ToString().Trim());
                                        string F5 = dataTable.Rows[retryCount][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][8].ToString().Trim());
                                        string F6 = dataTable.Rows[retryCount][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][9].ToString().Trim());
                                        string F7 = dataTable.Rows[retryCount][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][10].ToString().Trim());
                                        string F8 = dataTable.Rows[retryCount][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][11].ToString().Trim());
                                        string F9 = dataTable.Rows[retryCount][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][12].ToString().Trim());
                                        string F10 = dataTable.Rows[retryCount][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][13].ToString().Trim());
                                        string F11 = dataTable.Rows[retryCount][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][14].ToString().Trim());
                                        string F12 = dataTable.Rows[retryCount][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][15].ToString().Trim());

                                        string moddate = dataTable.Rows[retryCount][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string symptom_category_id = string.Empty;
                                        string categoryName = dataTable.Rows[retryCount][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][17].ToString().Trim());
                                        if (categoryName == null || !SymptomCategories.Keys.Contains(categoryName.ToUpper()))
                                            throw new Exception("Row does not have valid category");
                                        else
                                            symptom_category_id = SymptomCategories[categoryName.ToUpper()];

                                        SymptomCodes[symptomCode.ToUpper()] = Guid.NewGuid().ToString().Trim();
                                        command.CommandText = "insert into complicationmaster (ComplicationMasterId,ComplicationCode,Name,NameDefault,Priority,ComplicationCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        //  $"('{Guid.NewGuid().ToString()}','{symptomCode}','{name}','{nameDefault}',{priority},'{symptom_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}','{moddate}',null,'{adminUserId}')";
                                        command.CommandText += $"('{SymptomCodes[symptomCode.ToUpper()]}','{symptomCode}','{name}','{nameDefault}',{priority},'{symptom_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";

                                        int x = await command.ExecuteNonQueryAsync();
                                        insertedSymptomCodes.Add(symptomCode);
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{SymptomMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}";
                                        if (SymptomMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{SymptomMasterRecordsWithExceptionsDict.Keys.Count}";
                                        SymptomMasterRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {SymptomMasterRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSignsMaster()
        {
            // port data from SignMaster to signsmaster
            /*
                SignMasterId varchar(36) not null primary key,   
                SignCode varchar(36) not null,                              strCode 
                Name varchar(100) not null,                                 strName
                NameDefault varchar(100)                                    str_default
                Min int(3),
                Max int(3),
                Priority int(3),                                            numPriority
                 F1 varchar(100),                                           f1
                 F2 varchar(100),                                           f2
                 F3 varchar(100),                                           f3
                 F4 varchar(100),                                           f4
                 F5 varchar(100),                                           f5
                 F6 varchar(100),                                           f6
                 F7 varchar(100),                                           f7
                 F8 varchar(100),                                           f8
                 F9 varchar(100),                                           f9
                 F10 varchar(100),                                          f10
                 F11 varchar(100),                                          f11
                F12 varchar(100),                                           f12
                SignCategoryId varchar(36) not null,                     department
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                                       Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
             */

            GetAccessData("select strCode, strName, str_default, numPriority,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12, Mod_Date, department from SignMaster;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 2;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        List<string> insertedSignCodes = new List<string>();
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into signmaster (SignMasterId,SignCode,Name,NameDefault,Priority,SignCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            List<string> tempCompCodes = new List<string>();
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    string signCode = MySqlHelper.EscapeString(dataTable.Rows[i][0] == null ? null : dataTable.Rows[i][0].ToString().Trim());
                                    if (signCode == null || insertedSignCodes.Contains(signCode) || tempCompCodes.Contains(signCode))
                                        throw new Exception("Invalid SignCode. Either null or duplicate");

                                    string name = MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());

                                    string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());

                                    int priority = Convert.ToInt32(dataTable.Rows[i][3]);

                                    string F1 = dataTable.Rows[i][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][4].ToString().Trim());
                                    string F2 = dataTable.Rows[i][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][5].ToString().Trim());
                                    string F3 = dataTable.Rows[i][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][6].ToString().Trim());
                                    string F4 = dataTable.Rows[i][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][7].ToString().Trim());
                                    string F5 = dataTable.Rows[i][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][8].ToString().Trim());
                                    string F6 = dataTable.Rows[i][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][9].ToString().Trim());
                                    string F7 = dataTable.Rows[i][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][10].ToString().Trim());
                                    string F8 = dataTable.Rows[i][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][11].ToString().Trim());
                                    string F9 = dataTable.Rows[i][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][12].ToString().Trim());
                                    string F10 = dataTable.Rows[i][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][13].ToString().Trim());
                                    string F11 = dataTable.Rows[i][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][14].ToString().Trim());
                                    string F12 = dataTable.Rows[i][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][15].ToString().Trim());

                                    string moddate = dataTable.Rows[i][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string sign_category_id = string.Empty;
                                    string categoryName = dataTable.Rows[i][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[i][17].ToString().Trim());
                                    if (categoryName == null || !SignCategories.Keys.Contains(categoryName.ToUpper()))
                                        throw new Exception("Row does not have valid category");
                                    else
                                        sign_category_id = SignCategories[categoryName.ToUpper()];

                                    SignCodes[signCode.ToUpper()] = Guid.NewGuid().ToString();
                                    command.CommandText += $"('{SignCodes[signCode.ToUpper()]}','{signCode}','{name}','{nameDefault}',{priority},'{sign_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                    tempCompCodes.Add(signCode);
                                }
                                catch (Exception ex)
                                {
                                    string key = $"{dataTable.Rows[i][0].ToString().Trim()}";
                                    if (SignMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{dataTable.Rows[i][0].ToString().Trim()}/{SignMasterRecordsWithExceptionsDict.Keys.Count}";
                                    SignMasterRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                insertedSignCodes.AddRange(tempCompCodes);
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{SignMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 
                                tempCompCodes.Clear();
                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        string signCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0] == null ? null : dataTable.Rows[retryCount][0].ToString().Trim());
                                        if (signCode == null || insertedSignCodes.Contains(signCode))
                                            throw new Exception("Invalid SignCode. Either null or duplicate");

                                        string name = MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());

                                        string nameDefault = MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());

                                        int priority = Convert.ToInt32(dataTable.Rows[retryCount][3]);

                                        string F1 = dataTable.Rows[retryCount][4] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][4].ToString().Trim());
                                        string F2 = dataTable.Rows[retryCount][5] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][5].ToString().Trim());
                                        string F3 = dataTable.Rows[retryCount][6] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][6].ToString().Trim());
                                        string F4 = dataTable.Rows[retryCount][7] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][7].ToString().Trim());
                                        string F5 = dataTable.Rows[retryCount][8] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][8].ToString().Trim());
                                        string F6 = dataTable.Rows[retryCount][9] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][9].ToString().Trim());
                                        string F7 = dataTable.Rows[retryCount][10] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][10].ToString().Trim());
                                        string F8 = dataTable.Rows[retryCount][11] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][11].ToString().Trim());
                                        string F9 = dataTable.Rows[retryCount][12] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][12].ToString().Trim());
                                        string F10 = dataTable.Rows[retryCount][13] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][13].ToString().Trim());
                                        string F11 = dataTable.Rows[retryCount][14] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][14].ToString().Trim());
                                        string F12 = dataTable.Rows[retryCount][15] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][15].ToString().Trim());

                                        string moddate = dataTable.Rows[retryCount][16] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][16])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string sign_category_id = string.Empty;
                                        string categoryName = dataTable.Rows[retryCount][17] == null ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][17].ToString().Trim());
                                        if (categoryName == null || !SignCategories.Keys.Contains(categoryName.ToUpper()))
                                            throw new Exception("Row does not have valid category");
                                        else
                                            sign_category_id = SignCategories[categoryName.ToUpper()];

                                        SignCodes[signCode.ToUpper()] = Guid.NewGuid().ToString();
                                        command.CommandText = "insert into signmaster (SignMasterId,SignCode,Name,NameDefault,Priority,SignCategoryId,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        //                 $"('{Guid.NewGuid().ToString()}','{signCode}','{name}','{nameDefault}',{priority},'{sign_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}','{moddate}',null,'{adminUserId}')";
                                        command.CommandText += $"('{SignCodes[signCode.ToUpper()]}','{signCode}','{name}','{nameDefault}',{priority},'{sign_category_id}','{F1}','{F2}','{F3}','{F4}','{F5}','{F6}','{F7}','{F8}','{F9}','{F10}','{F11}','{F12}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";

                                        int x = await command.ExecuteNonQueryAsync();
                                        insertedSignCodes.Add(signCode);
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{SignMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}";
                                        if (SignMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{SignMasterRecordsWithExceptionsDict.Keys.Count}";
                                        SignMasterRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {SignMasterRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortInvestigationsMaster()
        {
            // port data from InvestigationMaster to investigationmaster
            /*
               TestNo varchar(36) not null primary key,     
                TestCode varchar(200) not null,             strTestCode
                Name varchar(200) not null,                 strName
                Description varchar(200),                   strDescription
                Min float(8,3),                             numMin
                Max float(8,3),                             numMax
                NRepeat smallint,                           numRepeatAfter
                Units varchar(200) ,                        strunits
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                       Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
                                                            numPriority - is stored in dict 
             */

            GetAccessData("select strTestCode, strName,strDescription,numMin,numMax, strunits, Mod_Date from InvestigationMaster order by strTestCode;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 50;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        List<string> insertedTestCodes = new List<string>();
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into investigationmaster (TestNo,TestCode,Name,Description,Min,Max,NRepeat,Units, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            List<string> tempCompCodes = new List<string>();
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    string guid = Guid.NewGuid().ToString();
                                    string testCode = MySqlHelper.EscapeString(dataTable.Rows[i][0] == null ? null : dataTable.Rows[i][0].ToString().Trim());
                                    if (testCode == null || insertedTestCodes.Contains(testCode) || tempCompCodes.Contains(testCode))
                                        throw new Exception("Invalid TestCode. Either null or duplicate");

                                    string name = MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());

                                    string desc = MySqlHelper.EscapeString(dataTable.Rows[i][2].ToString().Trim());

                                    double min = Convert.ToDouble(dataTable.Rows[i][3]);
                                    double max = Convert.ToDouble(dataTable.Rows[i][4]);

                                    string units = dataTable.Rows[i][5] == null ? null : dataTable.Rows[i][5].ToString().Trim();

                                    string moddate = dataTable.Rows[i][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{guid}','{testCode}','{name}','{desc}',{min},{max},0,'{units}',{locationId},'{defaultCreatedDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                    tempCompCodes.Add(testCode);
                                    TestCodes[testCode.ToUpper()] = guid;
                                }
                                catch (Exception ex)
                                {
                                    string key = $"{dataTable.Rows[i][0].ToString().Trim()}";
                                    if (InvestigationMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                        key = $"{dataTable.Rows[i][0].ToString().Trim()}/{InvestigationMasterRecordsWithExceptionsDict.Keys.Count}";
                                    InvestigationMasterRecordsWithExceptionsDict[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                insertedTestCodes.AddRange(tempCompCodes);
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvestigationMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {
                                // the batch of events could not be inserted successfully. 
                                // so insert individually 
                                foreach (var testcode in tempCompCodes)
                                {
                                    TestCodes.Remove(testcode.ToUpper()); // since they didnt get added 
                                }

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        tempCompCodes.Clear();

                                        string guid = Guid.NewGuid().ToString();
                                        string testCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0] == null ? null : dataTable.Rows[retryCount][0].ToString().Trim());
                                        if (testCode == null || insertedTestCodes.Contains(testCode))
                                            throw new Exception("Invalid TestCode. Either null or duplicate");
                                        else
                                        {
                                            TestCodes[testCode.ToUpper()] = guid;
                                        }
                                        string name = MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());

                                        string desc = MySqlHelper.EscapeString(dataTable.Rows[retryCount][2].ToString().Trim());

                                        double min = Convert.ToDouble(dataTable.Rows[retryCount][3]);
                                        double max = Convert.ToDouble(dataTable.Rows[retryCount][4]);

                                        string units = dataTable.Rows[retryCount][5] == null ? null : dataTable.Rows[retryCount][5].ToString().Trim();

                                        string moddate = dataTable.Rows[retryCount][6] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][6])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText = "insert into investigationmaster (TestNo,TestCode,Name,Description,Min,Max,NRepeat,Units, LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        command.CommandText += $"('{guid}','{testCode}','{name}','{desc}',{min},{max},0,'{units}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";

                                        int x = await command.ExecuteNonQueryAsync();
                                        insertedTestCodes.Add(testCode);
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvestigationMasterRecordsWithExceptionsDict.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}";
                                        if (InvestigationMasterRecordsWithExceptionsDict.Keys.Contains(key))
                                            key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{InvestigationMasterRecordsWithExceptionsDict.Keys.Count}";
                                        InvestigationMasterRecordsWithExceptionsDict[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvestigationMasterRecordsWithExceptionsDict.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public class Dept_Test_Mapping
        {
            public string DepartmentId { get; set; }
            public string TestNo { get; set; }
            public string TestCode { get; set; }
        }

        public async Task PortDept_Test_Priorities()
        {
            // port data from test_dept_priority table to dept_test_priorities table
            /*
              DepartmentId VARCHAR(36) NOT NULL,        department
              TestNo VARCHAR(36) NOT NULL,              strTestCode --  use in TestCodes dict to get testNo
              Priority smallint not null,               numPriority    
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not null
             */

            GetAccessData("select department, strTestCode, numPriority from test_dept_priority where department in ('DIAB','TRANSPLANT','NEPHRO','') order by department, strTestCode");

            List<Dept_Test_Mapping> dept_test_mappings = new List<Dept_Test_Mapping>();

            using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
            {
                await connection.OpenAsync();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;

                    // alternative method - map all departmentids to testids and set priority to default 999. then read from accessdb and populate each.
                    command.CommandText = " select DepartmentId, TestNo, TestCode from departments, investigationmaster;";
                    using (DbDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            Dept_Test_Mapping mapping = new Dept_Test_Mapping();
                            mapping.DepartmentId = reader["DepartmentId"].ToString().Trim();
                            mapping.TestNo = reader["TestNo"].ToString().Trim();
                            mapping.TestCode = reader["TestCode"].ToString().Trim();
                            dept_test_mappings.Add(mapping);
                        }
                    }

                    // foreach row in the datatable, check whether a mapping exists in dept_test_mappings that has the same departmentid and testcode. 
                    // if yes, then take the departmentid and testno from that mapping and the row's priority and save to table. Remove that mapping from the list. 

                    // after the foreach, if there are any mappings left, save them with priority 999. 

                    int total = 0;
                    string oldDepartment = string.Empty;
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        command.CommandText = "insert into dept_test_priorities (DepartmentId, TestNo, Priority, LocationId, CreatedDate, UpdatedDate, DeletedDate, UserId) values";

                        string departmentname = string.Empty;
                        string testCode = string.Empty;
                        try
                        {
                            departmentname = dataTable.Rows[i][0].ToString().Trim();
                            if (!oldDepartment.Equals(departmentname))
                            {
                                total = 0;
                                oldDepartment = departmentname;
                            }
                            string departmentId = string.Empty;
                            if (departmentname == "")
                                departmentId = Departments["GENERAL"];
                            else
                                departmentId = Departments[departmentname.ToUpper()];

                            testCode = dataTable.Rows[i][1].ToString().Trim();
                            //string testNo = string.Empty;
                            //if (TestCodes.Keys.Contains(testCode.ToUpper()))
                            //    testNo = TestCodes[testCode.ToUpper()];
                            //else
                            //    throw new Exception($"TestCode does not exist in investigationmaster table");

                            var mapping = (from dept_test_mapping in dept_test_mappings
                                           where dept_test_mapping.DepartmentId == departmentId && dept_test_mapping.TestCode.ToUpper() == testCode.ToUpper()
                                           select dept_test_mapping).FirstOrDefault();

                            if (mapping == null)
                                throw new Exception("The department or testcode do not exist in db");

                            int priority = Convert.ToInt32(dataTable.Rows[i][2]);
                            dept_test_mappings.Remove(mapping);

                            command.CommandText += $"('{departmentId}','{mapping.TestNo}', {priority},{locationId},'{defaultCreatedDate}',null,null,'{adminUserId}')";
                            int x = await command.ExecuteNonQueryAsync();
                            total += x;

                            Console.WriteLine($"Total Records inserted for {departmentname}:{total} ");

                        }
                        catch (Exception ex)
                        {
                            string key = $"{dataTable.Rows[i][1].ToString().Trim()}/{departmentname}";
                            if (InvalidDeptTestPriorities.Keys.Contains(key))
                                key = $"{dataTable.Rows[i][1].ToString().Trim()}/{departmentname}/{InvalidDeptTestPriorities.Keys.Count}";
                            InvalidDeptTestPriorities[key] = ex.Message;
                        }
                    }

                    if (dept_test_mappings.Count != 0)
                    {
                        try
                        {
                            foreach (var mapping in dept_test_mappings)
                            {
                                command.CommandText = $"insert into dept_test_priorities(DepartmentId, TestNo, Priority, LocationId, CreatedDate, UpdatedDate, DeletedDate, UserId) " +
                                    $"values ('{mapping.DepartmentId}','{mapping.TestNo}', {1000},{locationId},'{defaultCreatedDate}',null,null,'{adminUserId}')";
                                int x = await command.ExecuteNonQueryAsync();
                                Console.WriteLine($"Mapping not found in accessdb, but required in MYSQL: {mapping.DepartmentId} , {mapping.TestNo}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"ex");
                        }
                    }

                    Console.WriteLine($"Total Records read from accessdb: {dataTable.Rows.Count}");
                    Console.WriteLine($"Total Error Records :{InvalidDeptTestPriorities.Count} ");
                }
            }
        }

        public class Category_Test_Mapping
        {
            public string CategoryId { get; set; }
            public string TestNo { get; set; }
            public string TestCode { get; set; }
        }

        public async Task PortCategory_Test_Priorities()
        {
            // port data from investigationmaster table to category_test_priorities table
            /*
              CategoryId VARCHAR(36) NOT NULL,        department
              TestNo VARCHAR(36) NOT NULL,            strTestCode --  use in TestCodes dict to get testNo
              Priority smallint not null,             numPriority    
              LocationId int(3) unsigned NOT NULL,
              CreatedDate DATETIME NOT NULL,
              UpdatedDate DATETIME NULL,
              DeletedDate DATETIME NULL,
              UserId VARCHAR(36) not null
             */

            GetAccessData("select department, strTestCode, numPriority from InvestigationMaster order by department;");

            List<Category_Test_Mapping> catg_test_mappings = new List<Category_Test_Mapping>();

            using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
            {
                await connection.OpenAsync();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;

                    // alternative method - map all categoryids to testids and set priority to default 999. then read from accessdb and populate each.
                    command.CommandText = " select CategoryId, TestNo, TestCode from test_category, investigationmaster;";

                    using (DbDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            Category_Test_Mapping mapping = new Category_Test_Mapping();
                            mapping.CategoryId = reader["CategoryId"].ToString().Trim();
                            mapping.TestNo = reader["TestNo"].ToString().Trim();
                            mapping.TestCode = reader["TestCode"].ToString().Trim();
                            catg_test_mappings.Add(mapping);
                        }
                    }

                    // foreach row in the datatable, check whether a mapping exists in category_test_mappings that has the same categoryid and testcode. 
                    // if yes, then take the categoryid and testno from that mapping and the row's priority and save to table. Remove that mapping from the list. 

                    // after the foreach, if there are any mappings left, save them with priority  1000. 

                    int total = 0;
                    string oldDepartment = string.Empty;
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        command.CommandText = "insert into category_test_priorities (CategoryId, TestNo, Priority, LocationId, CreatedDate, UpdatedDate, DeletedDate, UserId) values";

                        string categoryname = string.Empty;
                        string testCode = string.Empty;
                        try
                        {
                            categoryname = dataTable.Rows[i][0].ToString().Trim();
                            if (!oldDepartment.Equals(categoryname))
                            {
                                total = 0;
                                oldDepartment = categoryname;
                            }
                            string categoryId = TestCategories[categoryname.ToUpper()];

                            testCode = dataTable.Rows[i][1].ToString().Trim();
                            //string testNo = string.Empty;
                            //if (TestCodes.Keys.Contains(testCode.ToUpper()))
                            //    testNo = TestCodes[testCode.ToUpper()];
                            //else
                            //    throw new Exception($"TestCode does not exist in investigationmaster table");

                            var mapping = (from catg_test_mapping in catg_test_mappings
                                           where catg_test_mapping.CategoryId == categoryId && catg_test_mapping.TestCode.ToUpper() == testCode.ToUpper()
                                           select catg_test_mapping).FirstOrDefault();

                            if (mapping == null)
                                throw new Exception("The category or testcode do not exist in db");

                            int priority = Convert.ToInt32(dataTable.Rows[i][2]);
                            catg_test_mappings.Remove(mapping);

                            command.CommandText += $"('{categoryId}','{mapping.TestNo}', {priority},{locationId},'{defaultCreatedDate}',null,null,'{adminUserId}')";
                            int x = await command.ExecuteNonQueryAsync();
                            total += x;

                            Console.WriteLine($"Total Records inserted for {categoryname}:{total} ");
                        }
                        catch (Exception ex)
                        {
                            string key = $"{dataTable.Rows[i][1].ToString().Trim()}/{categoryname}";
                            if (InvalidCatgTestPriorities.Keys.Contains(key))
                                key = $"{dataTable.Rows[i][1].ToString().Trim()}/{categoryname}/{InvalidCatgTestPriorities.Keys.Count}";
                            InvalidCatgTestPriorities[key] = ex.Message;
                        }
                    }

                    if (catg_test_mappings.Count != 0)
                    {
                        try
                        {
                            foreach (var mapping in catg_test_mappings)
                            {
                                command.CommandText = $"insert into category_test_priorities(CategoryId, TestNo, Priority, LocationId, CreatedDate, UpdatedDate, DeletedDate, UserId) " +
                                    $"values ('{mapping.CategoryId}','{mapping.TestNo}', {1000},{locationId},'{defaultCreatedDate}',null,null,'{adminUserId}')";
                                int x = await command.ExecuteNonQueryAsync();
                                Console.WriteLine($"Mapping not found in accessdb, but required in MYSQL: {mapping.CategoryId} , {mapping.TestNo}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"ex");
                        }
                    }

                    Console.WriteLine($"Total Records read from accessdb: {dataTable.Rows.Count}");
                    Console.WriteLine($"Total Error Records :{InvalidDeptTestPriorities.Count} ");
                }
            }
        }

        public async Task PortComplications()
        {
            /* Port from complications table to Complications 
            ComplicationId varchar(36) not null primary key,        
            PatientId int(10) unsigned not null,                numPatientid
            ComplicationMasterId varchar(36) not null,          strCode
            DateOfEntry datetime not null,                      DOE
            Level int(3),                                       strlevel
            Observation varchar(100),                           interpretation
            LocationId int(3) unsigned not null,
            CreatedDate datetime not null,                      strDay, strMonth, strYear
            UpdatedDate datetime,                               Mod_Date
            DeletedDate datetime,
            UserId varchar(36) not null
             */

            try
            {
                GetAccessData("select numPatientid,strCode, DOE,strlevel,interpretation,strDay, strMonth,strYear,Mod_Date from complications order by numPatientid;");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}");
                Console.WriteLine($"Make sure you have changed the name of level column to strlevel in accessdb complications table");
                InvalidComplications["All codes/All patientIds"] = e.Message;
            }

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 50;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        int patientId = 0;
                        string complicationMasterCode = string.Empty;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into complications (ComplicationId,PatientId,ComplicationMasterId,DateOfEntry,Level,Observation,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    patientId = Convert.ToInt32(dataTable.Rows[i][0]);

                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception("PatientId does not exist in DB");

                                    complicationMasterCode = dataTable.Rows[i][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());
                                    string complicationMasterId = string.Empty;
                                    if (complicationMasterCode != null && ComplicationCodes.Keys.Contains(complicationMasterCode.ToUpper()))
                                        complicationMasterId = ComplicationCodes[complicationMasterCode.ToUpper()];
                                    else
                                        throw new Exception("ComplicationCode does not exist in ComplicationMaster table");

                                    string dateOfEntry = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                    int level = Convert.ToInt32(dataTable.Rows[i][3]);

                                    string observation = dataTable.Rows[i][4] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][4].ToString().Trim());

                                    string createdDate = string.Empty;
                                    int day = Convert.ToInt32(dataTable.Rows[i][5]);
                                    string month = IsValidMonth(dataTable.Rows[i][6] == DBNull.Value ? null : dataTable.Rows[i][6].ToString().Trim());
                                    int year = Convert.ToInt32(dataTable.Rows[i][7]);
                                    if (day != 0 && month != null && year != 0)
                                    {
                                        createdDate = $"20{year}-{month}-{day} 00:00:00";
                                    }
                                    else
                                        createdDate = defaultCreatedDate;


                                    string moddate = dataTable.Rows[i][8] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][8])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{complicationMasterId}','{dateOfEntry}',{level},'{observation}',{locationId},'{createdDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    string code = dataTable.Rows[i][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());
                                    string key = $"{code}/{patientId}";
                                    if(InvalidComplications.Keys.Contains(key))
                                        key = $"{code}/{patientId}/{InvalidComplications.Keys.Count}";
                                    InvalidComplications[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidComplications.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        patientId = Convert.ToInt32(dataTable.Rows[retryCount][0]);

                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        complicationMasterCode = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string complicationMasterId = string.Empty;
                                        if (complicationMasterCode != null && ComplicationCodes.Keys.Contains(complicationMasterCode.ToUpper()))
                                            complicationMasterId = ComplicationCodes[complicationMasterCode.ToUpper()];
                                        else
                                            throw new Exception("ComplicationCode does not exist in ComplicationMaster table");

                                        string dateOfEntry = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        int level = Convert.ToInt32(dataTable.Rows[retryCount][3]);

                                        string observation = dataTable.Rows[retryCount][4] == DBNull.Value ? null : dataTable.Rows[retryCount][4].ToString().Trim();

                                        string createdDate = string.Empty;
                                        int day = Convert.ToInt32(dataTable.Rows[retryCount][5]);
                                        string month = IsValidMonth(dataTable.Rows[retryCount][6] == DBNull.Value ? null : dataTable.Rows[retryCount][6].ToString().Trim());
                                        int year = Convert.ToInt32(dataTable.Rows[retryCount][7]);
                                        if (day != 0 && month != null && year != 0)
                                        {
                                            createdDate = $"20{year}-{month}-{day} 00:00:00";
                                        }
                                        else
                                            createdDate = defaultCreatedDate;

                                        string moddate = dataTable.Rows[retryCount][8] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][8])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText = "insert into complications (ComplicationId,PatientId,ComplicationMasterId,DateOfEntry,Level,Observation,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{complicationMasterId}','{dateOfEntry}',{level},'{observation}',{locationId},'{createdDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidComplications.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string code = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string key = $"{code}/{patientId}";
                                        if (InvalidComplications.Keys.Contains(key))
                                            key = $"{code}/{patientId}/{InvalidComplications.Keys.Count}";
                                        InvalidComplications[key] = individualex.Message;
                                    }
                                }
                            }
                        }
                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvalidComplications.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSymptoms()
        {
            /* Port from symptoms table to Symptoms 
            SymptomId varchar(36) not null primary key,        
            PatientId int(10) unsigned not null,                numPatientid
            SymptomMasterId varchar(36) not null,               strCode
            DateOfEntry datetime not null,                      DOE
            Details varchar(100),                              str_default
            LocationId int(3) unsigned not null,
            CreatedDate datetime not null,                      strDay, strMonth, strYear
            UpdatedDate datetime,                               Mod_Date
            DeletedDate datetime,
            UserId varchar(36) not null
            */

            GetAccessData("select numPatientid,strCode, DOE,str_default,strDay, strMonth,strYear,Mod_Date from symptoms order by numPatientid;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 50;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        int patientId = 0;
                        string symptomMasterCode = string.Empty;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into symptoms (SymptomId,PatientId,SymptomMasterId,DateOfEntry,Details,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    patientId = Convert.ToInt32(dataTable.Rows[i][0]);

                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception("PatientId does not exist in DB");

                                    symptomMasterCode = dataTable.Rows[i][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());
                                    string symptomMasterId = string.Empty;
                                    if (symptomMasterCode != null && SymptomCodes.Keys.Contains(symptomMasterCode.ToUpper()))
                                        symptomMasterId = SymptomCodes[symptomMasterCode.ToUpper()];
                                    else
                                        throw new Exception("SymptomCode does not exist in SymptomsMaster table");

                                    string dateOfEntry = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string details = dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim());

                                    string createdDate = string.Empty;
                                    int day = Convert.ToInt32(dataTable.Rows[i][4]);
                                    string month = IsValidMonth(dataTable.Rows[i][5] == DBNull.Value ? null : dataTable.Rows[i][5].ToString().Trim());
                                    int year = Convert.ToInt32(dataTable.Rows[i][6]);
                                    if (day != 0 && month != null && year != 0)
                                    {
                                        createdDate = $"20{year}-{month}-{day} 00:00:00";
                                    }
                                    else
                                        createdDate = defaultCreatedDate;


                                    string moddate = dataTable.Rows[i][7] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][7])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{symptomMasterId}','{dateOfEntry}','{details}',{locationId},'{createdDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    string key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim())}/{patientId}";
                                    if (InvalidSymptoms.Keys.Contains(key))
                                        key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim())}/{patientId}/{InvalidSymptoms.Keys.Count}";
                                    InvalidSymptoms[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidSymptoms.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        patientId = Convert.ToInt32(dataTable.Rows[retryCount][0]);

                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        symptomMasterCode = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string symptomMasterId = string.Empty;
                                        if (symptomMasterCode != null && SymptomCodes.Keys.Contains(symptomMasterCode.ToUpper()))
                                            symptomMasterId = SymptomCodes[symptomMasterCode.ToUpper()];
                                        else
                                            throw new Exception("SymptomCode does not exist in SymptomMaster table");

                                        string dateOfEntry = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string details = dataTable.Rows[retryCount][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][3].ToString().Trim());

                                        string createdDate = string.Empty;
                                        int day = Convert.ToInt32(dataTable.Rows[retryCount][4]);
                                        string month = IsValidMonth(dataTable.Rows[retryCount][5] == DBNull.Value ? null : dataTable.Rows[retryCount][5].ToString().Trim());
                                        int year = Convert.ToInt32(dataTable.Rows[retryCount][6]);
                                        if (day != 0 && month != null && year != 0)
                                        {
                                            createdDate = $"20{year}-{month}-{day} 00:00:00";
                                        }
                                        else
                                            createdDate = defaultCreatedDate;

                                        string moddate = dataTable.Rows[retryCount][7] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][7])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText = "insert into symptoms (SymptomId,PatientId,SymptomMasterId,DateOfEntry,Details,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{symptomMasterId}','{dateOfEntry}','{details}',{locationId},'{createdDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidSymptoms.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string code = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string key = $"{code}/{patientId}";
                                        if (InvalidSymptoms.Keys.Contains(key))
                                            key = $"{MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim())}/{patientId}/{InvalidSymptoms.Keys.Count}";
                                        InvalidSymptoms[key] = individualex.Message;
                                    }
                                }
                            }
                        }
                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvalidSymptoms.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task PortSigns()
        {
            /* Port from signs table to Signs 
            SignId varchar(36) not null primary key,        
            PatientId int(10) unsigned not null,                numPatientid
            SignMasterId varchar(36) not null,                  strCode
            DateOfEntry datetime not null,                      DOE
            Details varchar(100),                              str_default
            LocationId int(3) unsigned not null,
            CreatedDate datetime not null,                      strDay, strMonth, strYear
            UpdatedDate datetime,                               Mod_Date
            DeletedDate datetime,
            UserId varchar(36) not null
            */

            GetAccessData("select numPatientid,strCode, DOE,str_default,strDay, strMonth,strYear,Mod_Date from signs order by numPatientid;");

            try
            {
                int totalRecordsInserted = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;

                        int insertRecordsInBatchesOf = 50;
                        int i = 0;
                        int j = 0;
                        bool doesCommandTextHaveRows = false;
                        int patientId = 0;
                        string signMasterCode = string.Empty;
                        while (i < dataTable.Rows.Count)
                        {
                            doesCommandTextHaveRows = false;
                            command.Parameters.Clear();
                            command.CommandText = "insert into signs (SignId,PatientId,SignMasterId,DateOfEntry,Details,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                            for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                            {
                                try
                                {
                                    patientId = Convert.ToInt32(dataTable.Rows[i][0]);

                                    if (!PatientIds.Contains(patientId))
                                        throw new Exception("PatientId does not exist in DB");

                                    signMasterCode = dataTable.Rows[i][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());
                                    string signMasterId = string.Empty;
                                    if (signMasterCode != null && SignCodes.Keys.Contains(signMasterCode.ToUpper()))
                                        signMasterId = SignCodes[signMasterCode.ToUpper()];
                                    else
                                        throw new Exception("SignCode does not exist in SignMaster table");

                                    string dateOfEntry = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                    string details = dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim());

                                    string createdDate = string.Empty;
                                    int day = Convert.ToInt32(dataTable.Rows[i][4]);
                                    string month = IsValidMonth(dataTable.Rows[i][5] == DBNull.Value ? null : dataTable.Rows[i][5].ToString().Trim());
                                    int year = Convert.ToInt32(dataTable.Rows[i][6]);
                                    if (day != 0 && month != null && year != 0)
                                    {
                                        createdDate = $"20{year}-{month}-{day} 00:00:00";
                                    }
                                    else
                                        createdDate = defaultCreatedDate;


                                    string moddate = dataTable.Rows[i][7] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][7])).ToString("yyyy-MM-dd hh:mm:ss");

                                    command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{signMasterId}','{dateOfEntry}','{details}',{locationId},'{createdDate}',";
                                    command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                    command.CommandText += $",null,'{adminUserId}')";
                                    command.CommandText += ",";
                                    doesCommandTextHaveRows = true;
                                }
                                catch (Exception ex)
                                {
                                    string code = dataTable.Rows[i][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][1].ToString().Trim());
                                    string key = $"{code}/{patientId}";
                                    if (InvalidSigns.Keys.Contains(key))
                                        key = $"{code}/{patientId}/{InvalidSigns.Keys.Count}";
                                    InvalidSigns[key] = ex.Message;
                                }

                                if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                {
                                    if (command.CommandText.EndsWith(","))
                                        command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                    command.CommandText += ";";
                                }

                            }
                            try
                            {
                                if (!doesCommandTextHaveRows)
                                    continue;
                                int x = await command.ExecuteNonQueryAsync();
                                totalRecordsInserted += x;
                                Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidSigns.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - i}");
                            }
                            catch (Exception ex)
                            {

                                int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                for (int count = 1; count <= j; retryCount++, count++)
                                {
                                    try
                                    {
                                        patientId = Convert.ToInt32(dataTable.Rows[retryCount][0]);

                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        signMasterCode = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string signMasterId = string.Empty;
                                        if (signMasterCode != null && SignCodes.Keys.Contains(signMasterCode.ToUpper()))
                                            signMasterId = SymptomCodes[signMasterCode.ToUpper()];
                                        else
                                            throw new Exception("SignCode does not exist in SignMaster table");

                                        string dateOfEntry = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string details = dataTable.Rows[retryCount][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][3].ToString().Trim());

                                        string createdDate = string.Empty;
                                        int day = Convert.ToInt32(dataTable.Rows[retryCount][4]);
                                        string month = IsValidMonth(dataTable.Rows[retryCount][5] == DBNull.Value ? null : dataTable.Rows[retryCount][5].ToString().Trim());
                                        int year = Convert.ToInt32(dataTable.Rows[retryCount][6]);
                                        if (day != 0 && month != null && year != 0)
                                        {
                                            createdDate = $"20{year}-{month}-{day} 00:00:00";
                                        }
                                        else
                                            createdDate = defaultCreatedDate;

                                        string moddate = dataTable.Rows[retryCount][7] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][7])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText = "insert into signs (SignId,PatientId,SignMasterId,DateOfEntry,Details,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                        command.CommandText += $"('{Guid.NewGuid().ToString()}',{patientId},'{signMasterId}','{dateOfEntry}','{details}',{locationId},'{createdDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;

                                        int x = await command.ExecuteNonQueryAsync();
                                        totalRecordsInserted += x;
                                        Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidSigns.Count} ### Total records inserted: {totalRecordsInserted} ### Total records left: {dataTable.Rows.Count - 1 - retryCount}");
                                    }
                                    catch (Exception individualex)
                                    {
                                        string code = dataTable.Rows[retryCount][1] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][1].ToString().Trim());
                                        string key = $"{code}/{patientId}";
                                        if (InvalidSigns.Keys.Contains(key))
                                            key = $"{code}/{patientId}/{InvalidSigns.Keys.Count}";
                                        InvalidSigns[key] = individualex.Message;
                                    }
                                }
                            }
                        }

                    }
                }
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvalidSigns.Count}");
                Console.WriteLine($"Total records read from AccessDB: {dataTable.Rows.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public class CountOfInvestigationRecords
        {
            public int totalRecordsInserted { get; set; }
            public int totalRecordsRead { get; set; }
        }

        public async Task PortInvestigations()
        {
            try
            {
                /* Port from Investigations table to investigations 
                InvestigationId varchar(36) not null primary key,
                TestNo varchar(36) not null,     	                    strTestCode - get testno from dict 	
                PatientId int(10) unsigned not null,                    numPatientId
                TestDate datetime not null,                             dtTestDate
                TestResult varchar(200),                                strTestResult
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                                   Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
                */

                #region Option 1 - patient id wise 
                List<int> investigationsPatientIds = new List<int>();

                #region Code for getting all the patientid records
                //GetAccessData("select distinct(numPatientId) from investigations where numPatientId is not null order by numPatientId;");
                //for (int i = 0; i < dataTable.Rows.Count; i++)
                //{
                //    investigationsPatientIds.Add(Convert.ToInt32(dataTable.Rows[i][0]));
                //}
                #endregion
                #region Code for getting particular user defined patient records
                string path = $"{AppDomain.CurrentDomain.BaseDirectory}PortData//investigationpatients.txt";
                using (StreamReader streamReader = new StreamReader(File.Open(path, FileMode.Open)))
                {
                    while (!streamReader.EndOfStream)
                    {
                        string id = await streamReader.ReadLineAsync();
                        // This check has been added to make sure blank lines do not get added as new entries in the table
                        if (!string.IsNullOrWhiteSpace(id))
                            investigationsPatientIds.Add(Convert.ToInt32(id.Trim()));
                    }
                }
                #endregion

                int totalRecordsInserted = 0;
                int totalRecordsReadFromAccessDB = 0;
                //port investigations patientwise- in batches of 100 patients 
                for (int patcount = 0; patcount < investigationsPatientIds.Count; patcount++) 
                {
                    string patientIds = investigationsPatientIds[patcount].ToString();

                    try
                    {
                        Console.WriteLine($"\n\nPatientIds: {patientIds}");
                        GetAccessData($"select strTestCode, numPatientId,dtTestDate, strTestResult,Mod_Date from investigations where numPatientId in ({patientIds}) order by numPatientId, strTestCode, dtTestDate, Mod_Date desc;");

                        totalRecordsReadFromAccessDB += dataTable.Rows.Count;
                    }
                    catch (Exception ex)
                    {
                        //Console.WriteLine(" \n \n Issue in reading from accessdb.Reading for individual patientids now: \n\n");
                        //string[] ids = patientIds.Split(',');
                        //foreach (var patientId in ids)
                        //{
                        //    Console.WriteLine($"\n Inserting records for patient: {patientId}");
                        //    CountOfInvestigationRecords countOfInvestigationRecords = new CountOfInvestigationRecords()
                        //    {
                        //        totalRecordsInserted = totalRecordsInserted,
                        //        totalRecordsRead = totalRecordsReadFromAccessDB
                        //    };

                        //    CountOfInvestigationRecords countForPatient = await PortInvestigationsPerPatient(Convert.ToInt32(patientId), countOfInvestigationRecords);

                        //    if (countForPatient != null)
                        //    {
                        //        totalRecordsInserted = countForPatient.totalRecordsInserted;
                        //        totalRecordsReadFromAccessDB += countForPatient.totalRecordsRead;
                        //    }
                        //}

                        string key = $"All testcodes/{patientIds}/all test dates";
                        InvalidInvestigations[key] = ex.Message;
                        continue;
                    }

                    using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                    {
                        using (MySqlCommand command = new MySqlCommand())
                        {
                            await connection.OpenAsync();
                            command.Connection = connection;

                            int insertRecordsInBatchesOf = 5000;
                            int i = 0;
                            int j = 0;
                            bool doesCommandTextHaveRows = false;
                            string savedcodetestdt = string.Empty;
                            DateTime? savedmoddate = null;
                            while (i < dataTable.Rows.Count)
                            {
                                doesCommandTextHaveRows = false;
                                command.Parameters.Clear();
                                command.CommandText = "insert into investigations (InvestigationId,TestNo, PatientId,TestDate,TestResult,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                                {
                                    try
                                    {
                                        string testCode = MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim());
                                        string testMasterId = string.Empty;
                                        if (TestCodes.Keys.Contains(testCode.ToUpper()))
                                            testMasterId = TestCodes[testCode.ToUpper()];
                                        else
                                            throw new Exception("TestCode does not exist in InvestigationMaster table");

                                        int patientId = Convert.ToInt32(dataTable.Rows[i][1]);
                                        if (patientId == 110)
                                        {

                                        }

                                        if (patientId < 0 || !PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        string testDate = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string result = dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim());

                                        string moddate = dataTable.Rows[i][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string deleteddate = null;
                                        // If the testcode/testdate exists in savedcodetestdt check if moddate is lesser than savedmoddate. 
                                        // if yes then set deleteddate = moddate and save + add to list 
                                        // not doing this for now ==> // else set deleted date of existing record -> add that to list and insert this 
                                        string newcodetestDate = $"{testCode}/{testDate}";
                                        if (savedcodetestdt.Equals(newcodetestDate))
                                        {
                                            deleteddate = defaultCreatedDate;
                                            DuplicateInvestigationsSetWithDeletedDate.Add($"PatientId: {patientId} ## TestCode: {testCode} ## DtDate: {testDate} ## ModDate: {moddate} <= SavedModDate: {savedmoddate?.ToString("yyyy-MM-dd hh:mm:ss")}");
                                        }
                                        else
                                        {
                                            savedcodetestdt = newcodetestDate;
                                            savedmoddate = dataTable.Rows[i][4] == DBNull.Value ? null : (DateTime?)(Convert.ToDateTime(dataTable.Rows[i][4]));
                                        }


                                        command.CommandText += $"('{Guid.NewGuid().ToString()}','{testMasterId}',{patientId},'{testDate}','{result}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null," : $"'{moddate}',";
                                        command.CommandText += deleteddate == null ? $"null":$"'{deleteddate}'";
                                        command.CommandText += $",'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;
                                    }
                                    catch (Exception ex)
                                    {
                                        try
                                        {
                                            string key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[i][1])}/{dataTable.Rows[i][2]}";
                                            if(InvalidInvestigations.Keys.Contains(key))
                                                key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[i][1])}/{dataTable.Rows[i][2]}/{InvalidInvestigations.Keys.Count}";
                                            InvalidInvestigations[key] = ex.Message;
                                        }
                                        catch (Exception invalidex)
                                        {
                                            Console.WriteLine("Exception while creating the key for invalid exception");
                                            Console.ReadLine();
                                        }
                                    }

                                    if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                    {
                                        if (command.CommandText.EndsWith(","))
                                            command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                        command.CommandText += ";";
                                    }
                                }
                                try
                                {
                                    if (!doesCommandTextHaveRows)
                                        continue;
                                    int x = await command.ExecuteNonQueryAsync();
                                    totalRecordsInserted += x;
                                    Console.WriteLine($"Records inserted:{x} ### ExceptionRecords:{InvalidInvestigations.Count}");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"In retry: ex:{ex.Message} inner:{ex.InnerException?.Message}");
                                    int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                    for (int count = 1; count <= j; retryCount++, count++)
                                    {
                                        try
                                        {
                                            string testCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim());
                                            string testMasterId = string.Empty;
                                            if (TestCodes.Keys.Contains(testCode.ToUpper()))
                                                testMasterId = TestCodes[testCode.ToUpper()];
                                            else
                                                throw new Exception("TestCode does not exist in InvestigationMaster table");

                                            int patientId = Convert.ToInt32(dataTable.Rows[retryCount][1]);
                                            if (patientId < 0 || !PatientIds.Contains(patientId))
                                                throw new Exception("PatientId does not exist in DB");

                                            string testDate = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                            string result = dataTable.Rows[retryCount][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][3].ToString().Trim());

                                            string moddate = dataTable.Rows[retryCount][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                            string deleteddate = null;
                                            // If the testcode/testdate exists in savedcodetestdt check if moddate is lesser than savedmoddate. 
                                            // if yes then set deleteddate = moddate and save + add to list 
                                            // not doing this for now ==> // else set deleted date of existing record -> add that to list and insert this 
                                            string newcodetestDate = $"{testCode}/{testDate}";
                                            if (savedcodetestdt.Equals(newcodetestDate))
                                            {
                                                deleteddate = defaultCreatedDate;
                                                DuplicateInvestigationsSetWithDeletedDate.Add($"PatientId: {patientId} ## TestCode: {testCode} ## DtDate: {testDate} ## ModDate: {moddate} < SavedModDate: {savedmoddate?.ToString("yyyy-MM-dd hh:mm:ss")}");
                                            }
                                            else
                                            {
                                                savedcodetestdt = newcodetestDate;
                                                savedmoddate = dataTable.Rows[retryCount][4] == DBNull.Value ? null : (DateTime?)(Convert.ToDateTime(dataTable.Rows[retryCount][4]));
                                            }

                                            command.CommandText = "insert into investigations (InvestigationId,TestNo, PatientId,TestDate,TestResult,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                            command.CommandText += $"('{Guid.NewGuid().ToString()}','{testMasterId}',{patientId},'{testDate}','{result}',{locationId},'{defaultCreatedDate}',";
                                            command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                            command.CommandText += deleteddate == null ? $"null" : $"'{deleteddate}'";
                                            command.CommandText += $",'{adminUserId}')";
                                            doesCommandTextHaveRows = true;

                                            int x = await command.ExecuteNonQueryAsync();
                                            totalRecordsInserted += x;

                                            Console.WriteLine($"Records inserted:{x} ### PatientId: {patientId} ### ExceptionRecords:{InvalidInvestigations.Count}");
                                        }
                                        catch (Exception individualex)
                                        {
                                            try
                                            {
                                                string key = $"{dataTable.Rows[retryCount][0].ToString().Trim()}/{Convert.ToInt32(dataTable.Rows[retryCount][1])}/{dataTable.Rows[retryCount][2]}";
                                                if (InvalidInvestigations.Keys.Contains(key))
                                                    key = $"{MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[retryCount][1])}/{dataTable.Rows[retryCount][2]}/{InvalidInvestigations.Keys.Count}";
                                                InvalidInvestigations[key] = individualex.Message;
                                            }
                                            catch (Exception invalidex)
                                            {
                                                Console.WriteLine("Exception thrown while creating key for invalid in retry");
                                                Console.ReadLine();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Console.WriteLine($"Records read from AccessDB in this cycle: {dataTable.Rows.Count}");
                    Console.WriteLine($"Total records read till now: {totalRecordsReadFromAccessDB}");
                    Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                    Console.WriteLine($"Total exception records: {InvalidInvestigations.Count}");
                }
                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
        }

        public async Task<CountOfInvestigationRecords> PortInvestigationsPerPatient(int PatientId, CountOfInvestigationRecords countOfInvestigationRecords)
        {
            try
            {
                /* Port from Investigations table to investigations 
                InvestigationId varchar(36) not null primary key,
                TestNo varchar(36) not null,     	                    strTestCode - get testno from dict 	
                PatientId int(10) unsigned not null,                    numPatientId
                TestDate datetime not null,                             dtTestDate
                TestResult varchar(200),                                strTestResult
                LocationId int(3) unsigned not null,
                CreatedDate datetime not null,
                UpdatedDate datetime,                                   Mod_Date
                DeletedDate datetime,
                UserId varchar(36) not null
                */

                //port investigations patientwise- in batches of 100 patients 
                int totalRecordsReadFromAccessDB = 0;
                int totalRecordsInserted = countOfInvestigationRecords.totalRecordsInserted;
                try
                {
                    GetAccessData($"select strTestCode, numPatientId,dtTestDate, strTestResult,Mod_Date from investigations where numPatientId in ({PatientId}) order by strTestCode;");
                    totalRecordsReadFromAccessDB += dataTable.Rows.Count;

                    using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                    {
                        using (MySqlCommand command = new MySqlCommand())
                        {
                            await connection.OpenAsync();
                            command.Connection = connection;

                            int insertRecordsInBatchesOf = 5000;
                            int i = 0;
                            int j = 0;
                            bool doesCommandTextHaveRows = false;
                            while (i < dataTable.Rows.Count)
                            {
                                doesCommandTextHaveRows = false;
                                command.Parameters.Clear();
                                command.CommandText = "insert into investigations (InvestigationId,TestNo, PatientId,TestDate,TestResult,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                for (j = 0; i < dataTable.Rows.Count && j < insertRecordsInBatchesOf; i++, j++)
                                {
                                    try
                                    {
                                        string testCode = MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim());
                                        string testMasterId = string.Empty;
                                        if (TestCodes.Keys.Contains(testCode.ToUpper()))
                                            testMasterId = TestCodes[testCode.ToUpper()];
                                        else
                                            throw new Exception("TestCode does not exist in InvestigationMaster table");

                                        int patientId = Convert.ToInt32(dataTable.Rows[i][1]);
                                        if (!PatientIds.Contains(patientId))
                                            throw new Exception("PatientId does not exist in DB");

                                        string testDate = dataTable.Rows[i][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                        string result = dataTable.Rows[i][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[i][3].ToString().Trim());

                                        string moddate = dataTable.Rows[i][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[i][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                        command.CommandText += $"('{Guid.NewGuid().ToString()}','{testMasterId}',{patientId},'{testDate}','{result}',{locationId},'{defaultCreatedDate}',";
                                        command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                        command.CommandText += $",null,'{adminUserId}')";
                                        command.CommandText += ",";
                                        doesCommandTextHaveRows = true;
                                    }
                                    catch (Exception ex)
                                    {
                                        try
                                        {
                                            string key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[i][1])}/{dataTable.Rows[i][2]}";
                                            if (InvalidInvestigations.Keys.Contains(key))
                                                key = $"{MySqlHelper.EscapeString(dataTable.Rows[i][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[i][1])}/{dataTable.Rows[i][2]}/{InvalidInvestigations.Keys.Count}";
                                            InvalidInvestigations[key] = ex.Message;
                                        }
                                        catch (Exception invalidex)
                                        {
                                            Console.WriteLine("exception on key creation in single patient investigations insertion");
                                        }
                                    }

                                    if (i == (dataTable.Rows.Count - 1) || j == (insertRecordsInBatchesOf - 1))
                                    {
                                        if (command.CommandText.EndsWith(","))
                                            command.CommandText = command.CommandText.Remove(command.CommandText.Length - 1, 1);
                                        command.CommandText += ";";
                                    }
                                }
                                try
                                {
                                    if (!doesCommandTextHaveRows)
                                        continue;
                                    int x = await command.ExecuteNonQueryAsync();
                                    totalRecordsInserted += x;
                                    Console.WriteLine($"PatientId: {PatientId} ### Records inserted:{x} ### ExceptionRecords:{InvalidInvestigations.Count}");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"In retry: ex:{ex.Message} inner:{ex.InnerException?.Message}");
                                    int retryCount = i - j;  // if less than 'insertRecordsInBatchesOf' records were to be inserted, eg. 3100 records, then we should subtract only 3100 not 5000 
                                    for (int count = 1; count <= j; retryCount++, count++)
                                    {
                                        try
                                        {
                                            string testCode = MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim());
                                            string testMasterId = string.Empty;
                                            if (TestCodes.Keys.Contains(testCode.ToUpper()))
                                                testMasterId = TestCodes[testCode.ToUpper()];
                                            else
                                                throw new Exception("TestCode does not exist in InvestigationMaster table");

                                            int patientId = Convert.ToInt32(dataTable.Rows[retryCount][1]);
                                            if (!PatientIds.Contains(patientId))
                                                throw new Exception("PatientId does not exist in DB");

                                            string testDate = dataTable.Rows[retryCount][2] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][2])).ToString("yyyy-MM-dd hh:mm:ss");

                                            string result = dataTable.Rows[retryCount][3] == DBNull.Value ? null : MySqlHelper.EscapeString(dataTable.Rows[retryCount][3].ToString().Trim());

                                            string moddate = dataTable.Rows[retryCount][4] == DBNull.Value ? null : (Convert.ToDateTime(dataTable.Rows[retryCount][4])).ToString("yyyy-MM-dd hh:mm:ss");

                                            command.CommandText = "insert into investigations (InvestigationId,TestNo, PatientId,TestDate,TestResult,LocationId,CreatedDate,UpdatedDate,DeletedDate,UserId) values";
                                            command.CommandText += $"('{Guid.NewGuid().ToString()}','{testMasterId}',{patientId},'{testDate}','{result}',{locationId},'{defaultCreatedDate}',";
                                            command.CommandText += moddate == null ? $"null" : $"'{moddate}'";
                                            command.CommandText += $",null,'{adminUserId}')";
                                            doesCommandTextHaveRows = true;

                                            int x = await command.ExecuteNonQueryAsync();
                                            totalRecordsInserted += x;
                                            Console.WriteLine($"Records inserted:{x} ### PatientId: {PatientId} ### ExceptionRecords:{InvalidInvestigations.Count}");
                                        }
                                        catch (Exception individualex)
                                        {
                                            try
                                            {
                                                string key = $"{MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[retryCount][1])}/{dataTable.Rows[retryCount][2]}";
                                                if (InvalidInvestigations.Keys.Contains(key))
                                                    key = $"{MySqlHelper.EscapeString(dataTable.Rows[retryCount][0].ToString().Trim())}/{Convert.ToInt32(dataTable.Rows[retryCount][1])}/{dataTable.Rows[retryCount][2]}/{InvalidInvestigations.Keys.Count}";
                                                InvalidInvestigations[key] = individualex.Message;
                                            }
                                            catch (Exception invalidex)
                                            {
                                                Console.WriteLine("Exception in retry of single patient investigation while creating key");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    string key = $"All testcodes/{PatientId}/all test dates";
                    InvalidInvestigations[key] = ex.Message;
                    return null;
                }

                Console.WriteLine($"records read from AccessDB in this cycle: {dataTable.Rows.Count}");
                Console.WriteLine($"Total records read from AccessDB: {totalRecordsReadFromAccessDB}");
                Console.WriteLine($"Total records inserted: {totalRecordsInserted}");
                Console.WriteLine($"Total exception records: {InvalidInvestigations.Count}");

                return new CountOfInvestigationRecords()
                {
                    totalRecordsInserted = totalRecordsInserted,
                    totalRecordsRead = totalRecordsReadFromAccessDB
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
            return null;
        }

        public async Task CheckInvestigationsValidity()
        {
            int total = 0;
            for (int i = 0; i < PatientIds.Count; i++)
            {
                int patientId = PatientIds[i];

                GetAccessData($"select count(*) from investigations where numPatientId = {patientId};");
                int accessDBCount = Convert.ToInt32(dataTable.Rows[0][0]);
                int mysqlCount = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = $"select count(*) as count from investigations where PatientId = {patientId}";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                mysqlCount = Convert.ToInt32(reader["count"]);
                            }
                        }
                    }
                }

                if (accessDBCount != mysqlCount)
                {
                    total += (mysqlCount - accessDBCount);
                    Console.WriteLine($"PatientId: {patientId}  ### AccessDBCount: {accessDBCount} ### MysqlCount: {mysqlCount} ### diff {mysqlCount - accessDBCount}");
                }
            }
            Console.WriteLine($"total: {total}");
        }

        public async Task CheckEventsValidity()
        {
            int total = 0;
            for (int i = 0; i < PatientIds.Count; i++)
            {
                int patientId = PatientIds[i];

                GetAccessData($"select count(*) from events where numPatientId = {patientId};");
                int accessDBCount = Convert.ToInt32(dataTable.Rows[0][0]);
                int mysqlCount = 0;
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        await connection.OpenAsync();
                        command.Connection = connection;
                        command.CommandText = $"select count(*) as count from events where PatientId = {patientId}";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                mysqlCount = Convert.ToInt32(reader["count"]);
                            }
                        }
                    }
                }

                if (accessDBCount != mysqlCount)
                {
                    total += (mysqlCount - accessDBCount);
                    Console.WriteLine($"PatientId: {patientId}  ### AccessDBCount: {accessDBCount} ### MysqlCount: {mysqlCount} ### diff {mysqlCount - accessDBCount}");
                }
            }
            Console.WriteLine($"total: {total}");
        }



        #region Get Details Required to port only specific sync level 
        public async Task GetDetailsRequiredForSyncLevel2()
        {
            // to be able to run sync level 2 successfully, we need the adminuserid, roleids and userids
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    await connection.OpenAsync();

                    // we already know that leadows will be assigned admin role.
                    // Since usersinroles hasn't been populated yet, hence we can just take the userid directly from users for now.

                    #region Code to get all userids including adminuserid
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select UserId, UserName from users";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {


                            while (await reader.ReadAsync())
                            {
                                string userName = Convert.ToString(reader["UserName"]);
                                if (userName == "xxx")
                                    Users["xxx"] = Convert.ToString(reader["UserId"]);
                                else if (userName == "Ashay")
                                    Users["Ashay"] = Convert.ToString(reader["UserId"]);
                                else if (userName == "Leadows")
                                {
                                    Users["Leadows"] = Convert.ToString(reader["UserId"]);
                                    adminUserId = Users["Leadows"];
                                }
                            }
                        }
                    }
                    #endregion

                    #region Code to get all roleids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select RoleId, RoleName from roles where RoleName in ('Admin', 'Editor','AppMaster')";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string roleName = Convert.ToString(reader["RoleName"]);
                                string roleId = Convert.ToString(reader["RoleId"]);
                                switch (roleName)
                                {
                                    case "Admin":
                                        Roles["Admin"] = roleId;
                                        break;
                                    case "Editor":
                                        Roles["Editor"] = roleId;
                                        break;
                                    case "AppMaster":
                                        Roles["AppMaster"] = roleId;
                                        break;
                                }
                            }
                        }
                    }
                    #endregion
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task GetDetailsRequiredForSyncLevel3()
        {
            // to be able to run sync level 3 successfully, we need the:
            // adminuserid
            // all medicinegroups in their dict
            // all depts in their dict
            // dataentrytypes
            // shortcutkeys
            // complicationcategories
            // symptomccategories
            // signcategories
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    await connection.OpenAsync();
                    #region Code to get admin userid
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select UserId from usersinroles, roles where usersinroles.RoleId=roles.RoleId and roles.RoleName='Admin';";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                adminUserId = Convert.ToString(reader["UserId"]);
                            }
                        }
                    }
                    #endregion

                    #region Code to get all medicine group ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select GroupId, GroupName from medicinegroups";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string groupName = Convert.ToString(reader["GroupName"]);
                                string groupId = Convert.ToString(reader["GroupId"]);
                                switch (groupName)
                                {
                                    case "AKT":
                                        MedicineGroups["AKT"].Id = groupId;
                                        break;
                                    case "ANTIBIOTIC":
                                        MedicineGroups["ANTIBIOTIC"].Id = groupId;
                                        break;
                                    case "CNS":
                                        MedicineGroups["CNS"].Id = groupId;
                                        break;
                                    case "CVS":
                                        MedicineGroups["CVS"].Id = groupId;
                                        MedicineGroups["CARDIAC"].Id = groupId;
                                        break;

                                    case "DIAB":
                                        MedicineGroups["DIAB"].Id = groupId;
                                        MedicineGroups["GDIABEN"].Id = groupId;
                                        break;
                                    case "GEN":
                                        MedicineGroups["GE"].Id = groupId;
                                        MedicineGroups["GEN"].Id = groupId;
                                        MedicineGroups["GENGEN"].Id = groupId;
                                        MedicineGroups["GENN"].Id = groupId;
                                        break;
                                    case "HD":
                                        MedicineGroups["HD"].Id = groupId;
                                        break;
                                    case "HTN":
                                        MedicineGroups["HBP"].Id = groupId;
                                        MedicineGroups["HTN"].Id = groupId;
                                        break;
                                    case "NEPHRO":
                                        MedicineGroups["NEPCVSHRO"].Id = groupId;
                                        MedicineGroups["NEPHRO"].Id = groupId;
                                        break;
                                    case "TB":
                                        MedicineGroups["TB"].Id = groupId;
                                        break;
                                    case "TRANSPLANT":
                                        MedicineGroups["TRANSPLANT"].Id = groupId;
                                        break;
                                }
                            }
                        }
                    }

                    #endregion

                    #region Code to get all departments
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select DepartmentId, DepartmentName from departments";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string deptName = Convert.ToString(reader["DepartmentName"]);
                                string deptId = Convert.ToString(reader["DepartmentId"]);
                                Departments[deptName.ToUpper()] = deptId;
                                //switch (deptName)
                                //{
                                //    case "DIAB":
                                //        Departments["DIAB"] = deptId;
                                //        break;
                                //    case "TRANSPLANT":
                                //        Departments["TRANSPLANT"] = deptId;
                                //        break;
                                //    case "NEPHRO":
                                //        Departments["NEPHRO"] = deptId;
                                //        break;
                                //    case "GENERAL":
                                //        Departments["GENERAL"] = deptId;
                                //        break;
                                //}
                            }
                        }
                    }
                    #endregion

                    #region Code to get all dataentrytypes
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select Id, DataEntryType from shortcutdataentrytypes";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string type = Convert.ToString(reader["DataEntryType"]);
                                string id = Convert.ToString(reader["Id"]);
                                ShortcutDataEntryTypes[type] = id;
                                //switch (type)
                                //{
                                //    case "Patient Form":
                                //        Departments["Patient Form"] = id;
                                //        break;
                                //    case "Medicine Dosage":
                                //        Departments["Medicine Dosage"] = id;
                                //        break;
                                //    case "Test":
                                //        Departments["Test"] = id;
                                //        break;
                                //    case "Illness":
                                //        Departments["Illness"] = id;
                                //        break;
                                //    case "Events":
                                //        Departments["Events"] = id;
                                //        break;
                                //}
                            }
                        }
                    }
                    #endregion

                    #region Code to get all shortcutkeys
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select Id, KeyName from shortcutkeys";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string keyName = Convert.ToString(reader["KeyName"]);
                                string id = Convert.ToString(reader["Id"]);
                                ShortcutKeys[keyName] = id;
                                //switch (keyName)
                                //{
                                //    case "F1":
                                //        ShortcutKeys["F1"] = id;
                                //        break;
                                //    case "F2":
                                //        Departments["F2"] = id;
                                //        break;
                                //    case "F3":
                                //        Departments["F3"] = id;
                                //        break;
                                //    case "F4":
                                //        Departments["F4"] = id;
                                //        break;
                                //    case "F5":
                                //        Departments["F5"] = id;
                                //        break;
                                //    case "F6":
                                //        Departments["F6"] = id;
                                //        break;
                                //    case "F7":
                                //        Departments["F7"] = id;
                                //        break;
                                //    case "F8":
                                //        Departments["F8"] = id;
                                //        break;
                                //    case "F9":
                                //        Departments["F9"] = id;
                                //        break;
                                //    case "F10":
                                //        Departments["F10"] = id;
                                //        break;
                                //    case "F11":
                                //        Departments["F11"] = id;
                                //        break;
                                //    case "F12":
                                //        Departments["F12"] = id;
                                //        break;
                                //}
                            }
                        }
                    }
                    #endregion

                    #region Code to get all complication category ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select CategoryId, CategoryName from complications_category;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string compCatgName = Convert.ToString(reader["CategoryName"]).Trim();
                                string compCatgId = Convert.ToString(reader["CategoryId"]);
                                ComplicationCategories[compCatgName.ToUpper()] = compCatgId;
                            }
                        }
                    }
                    #endregion

                    #region Code to get all symptom category ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select CategoryId, CategoryName from symptoms_category;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string compCatgName = Convert.ToString(reader["CategoryName"]).Trim();
                                string compCatgId = Convert.ToString(reader["CategoryId"]);
                                SymptomCategories[compCatgName.ToUpper()] = compCatgId;
                            }
                        }
                    }
                    #endregion

                    #region Code to get all sign category ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select CategoryId, CategoryName from signs_category;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string compCatgName = Convert.ToString(reader["CategoryName"]).Trim();
                                string compCatgId = Convert.ToString(reader["CategoryId"]);
                                SignCategories[compCatgName.ToUpper()] = compCatgId;
                            }
                        }
                    }
                    #endregion


                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public async Task GetDetailsRequiredForSyncLevel4()
        {
            // we need adminuserid
            // departments
            // formformats
            // patientids
            // medicineids
            // test categories
            // investigationmaster testno- testcode mapping (with priority)
            // complicationmastercodes
            // signcodes
            // symptomcodes

            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionStringForMySql))
                {
                    await connection.OpenAsync();
                    #region Code to get admin userid
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select UserId from usersinroles, roles where usersinroles.RoleId=roles.RoleId and roles.RoleName='Admin';";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                adminUserId = Convert.ToString(reader["UserId"]).Trim();
                            }
                        }
                    }
                    #endregion

                    #region Code to get all departments
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select DepartmentId, DepartmentName from departments";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string deptName = Convert.ToString(reader["DepartmentName"]).Trim();
                                string deptId = Convert.ToString(reader["DepartmentId"]);
                                Departments[deptName.ToUpper()] = deptId;
                                //switch (deptName)
                                //{
                                //    case "DIAB":
                                //        Departments["DIAB"] = deptId;
                                //        break;
                                //    case "TRANSPLANT":
                                //        Departments["TRANSPLANT"] = deptId;
                                //        break;
                                //    case "NEPHRO":
                                //        Departments["NEPHRO"] = deptId;
                                //        break;
                                //    case "GENERAL":
                                //        Departments["GENERAL"] = deptId;
                                //        break;
                                //}
                            }
                        }
                    }
                    #endregion

                    #region Code to get all formformats
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select FormFormatId, FormName from formformats";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string name = Convert.ToString(reader["FormName"]).Trim();
                                string id = Convert.ToString(reader["FormFormatId"]);
                                FormFormats[name.ToUpper()] = id;
                            }
                        }
                    }
                    #endregion

                    #region Code to get patientids 
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select PatientId from patients";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                PatientIds.Add(Convert.ToInt32(reader["PatientId"]));
                            }
                        }
                    }
                    #endregion

                    #region Code to get all medicine group ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;

                        command.CommandText = "select GroupId, GroupName from medicinegroups";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string groupName = Convert.ToString(reader["GroupName"]);
                                string groupId = Convert.ToString(reader["GroupId"]);
                                switch (groupName)
                                {
                                    case "AKT":
                                        MedicineGroups["AKT"].Id = groupId;
                                        break;
                                    case "ANTIBIOTIC":
                                        MedicineGroups["ANTIBIOTIC"].Id = groupId;
                                        break;
                                    case "CNS":
                                        MedicineGroups["CNS"].Id = groupId;
                                        break;
                                    case "CVS":
                                        MedicineGroups["CVS"].Id = groupId;
                                        MedicineGroups["CARDIAC"].Id = groupId;
                                        break;

                                    case "DIAB":
                                        MedicineGroups["DIAB"].Id = groupId;
                                        MedicineGroups["GDIABEN"].Id = groupId;
                                        break;
                                    case "GEN":
                                        MedicineGroups["GE"].Id = groupId;
                                        MedicineGroups["GEN"].Id = groupId;
                                        MedicineGroups["GENGEN"].Id = groupId;
                                        MedicineGroups["GENN"].Id = groupId;
                                        break;
                                    case "HD":
                                        MedicineGroups["HD"].Id = groupId;
                                        break;
                                    case "HTN":
                                        MedicineGroups["HBP"].Id = groupId;
                                        MedicineGroups["HTN"].Id = groupId;
                                        break;
                                    case "NEPHRO":
                                        MedicineGroups["NEPCVSHRO"].Id = groupId;
                                        MedicineGroups["NEPHRO"].Id = groupId;
                                        break;
                                    case "TB":
                                        MedicineGroups["TB"].Id = groupId;
                                        break;
                                    case "TRANSPLANT":
                                        MedicineGroups["TRANSPLANT"].Id = groupId;
                                        break;
                                }
                            }
                        }
                    }

                    #endregion

                    #region Code to get all medicineids
                    // Here we want to map medicineno from the accessdb to medicineid in the current db.
                    // so read from accessDB strMedicineName and iMedicineNo 
                    // get its corresponding medicineid from mysql based on medicinename + power + type + department
                    // VVV Imp-> there can be more than 1 medicine with the same name, but a combination of all 4 parameters will only have one med. 
                    GetAccessData("select strMedicineName, strPower, cType, department ,iMedicineNo from MedicineMaster;");

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        try
                        {
                            string medicineName = dataTable.Rows[i][0].ToString().Trim();

                            string strPower = dataTable.Rows[i][1].ToString().Trim();
                            string type = dataTable.Rows[i][2].ToString().Trim();
                            string oldType = type;
                            if (oldType == null)
                            {
                                type = "Misc";
                            }
                            else
                            {
                                type = type.Trim().ToLower();
                                if (type.Equals("cap") || type.Equals("capsule"))
                                    type = "Capsule";
                                else if (type.Equals("tab") || type.Equals("tablet"))
                                    type = "Tablet";
                                else if (!string.Equals(type, "injection") && !type.Equals("syrup") && !type.Equals("misc"))
                                {
                                    type = "Misc";
                                }
                            }

                            string dept = dataTable.Rows[i][3].ToString().Trim();
                            if (string.IsNullOrWhiteSpace(dept))
                                dept = "GEN";
                            string groupid = MedicineGroups[dept].Id;

                            int medicineNo = Convert.ToInt32(dataTable.Rows[i][4]);
                            using (MySqlCommand command = new MySqlCommand())
                            {
                                command.Connection = connection;

                                command.CommandText = $"select MedicineId from medicines where MedicineName = '{medicineName}' and Power='{strPower}' and Type='{type}' and GroupId='{groupid}'";
                                using (DbDataReader reader = await command.ExecuteReaderAsync())
                                {
                                    if (await reader.ReadAsync())
                                    {
                                        MedicineNo_MedicineId_Mapping[medicineNo] = reader["MedicineId"].ToString().Trim();
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {

                        }
                    }
                    #endregion

                    #region Code to get all test category ids
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select CategoryId, CategoryName from test_category;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string compCatgName = Convert.ToString(reader["CategoryName"]).Trim();
                                string compCatgId = Convert.ToString(reader["CategoryId"]).Trim();
                                TestCategories[compCatgName.ToUpper()] = compCatgId;
                            }
                        }
                    }
                    #endregion

                    #region Code to get testno - testcode mappings from investigationmaster with priority
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select TestNo, TestCode from investigationmaster;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string testCode = Convert.ToString(reader["TestCode"]).Trim();
                                string testNo = Convert.ToString(reader["TestNo"]).Trim();
                                TestCodes[testCode.ToUpper()] = testNo; // made toUpper as cases can differ but they mean the same code 
                            }
                        }
                    }
                    #endregion

                    #region Code to get all complication codes
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select ComplicationMasterId, ComplicationCode from complicationmaster;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string compMasterId = Convert.ToString(reader["ComplicationMasterId"]).Trim();
                                string compCode = Convert.ToString(reader["ComplicationCode"]).Trim();
                                ComplicationCodes[compCode.ToUpper()] = compMasterId;
                            }
                        }
                    }
                    #endregion

                    #region Code to get all sign codes
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select SignMasterId, SignCode from signmaster;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string signMasterId = Convert.ToString(reader["SignMasterId"]).Trim();
                                string signCode = Convert.ToString(reader["SignCode"]).Trim();
                                SignCodes[signCode.ToUpper()] = signMasterId;
                            }
                        }
                    }
                    #endregion

                    #region Code to get all symptom codes
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select SymptomMasterId, SymptomCode from symptomsmaster;";
                        using (DbDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string symptomMasterId = Convert.ToString(reader["SymptomMasterId"]).Trim();
                                string symptomCode = Convert.ToString(reader["SymptomCode"]).Trim();
                                SymptomCodes[symptomCode.ToUpper()] = symptomMasterId;
                            }
                        }
                    }
                    #endregion
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        #endregion
    }

    class Program
    {
        private static async void PortAllData()
        {
            DataPorting dataPorting = new DataPorting();
            // sync level 0 
            Console.WriteLine("Started Porting Locations");
            await dataPorting.PortLocations();
            Console.WriteLine("Completed Porting Locations");

            ////sync level 1
            Console.WriteLine("Started Porting Roles");
            await dataPorting.PortRoles();
            Console.WriteLine("Completed Porting Roles");
            Console.WriteLine("Started Porting Users ");
            await dataPorting.PortUsers();
            Console.WriteLine("Completed Porting Users ");

            //// sync level 2
            Console.WriteLine("Started Porting UsersInRoles");
            await dataPorting.PortUsersInRoles();
            Console.WriteLine("Completed Porting UsersInRoles");
            Console.WriteLine("Started Porting Groups");
            await dataPorting.PortMedicineGroups();
            Console.WriteLine("Completed Porting Groups");
            Console.WriteLine("Started Porting Departments");
            await dataPorting.PortPatientDepartments();
            Console.WriteLine("Completed Porting Departments");
            Console.WriteLine("Started Porting DataEntryTypes");
            await dataPorting.PortShortcutDataEntryTypes();
            Console.WriteLine("Completed Porting DataEntryTypes");
            Console.WriteLine("Started Porting ShortcutKeys");
            await dataPorting.PortShortcutKeys();
            Console.WriteLine("Completed Porting ShortcutKeys");

            Console.WriteLine("Started Porting Complication Categories");
            await dataPorting.PortComplicationCategories();
            Console.WriteLine("Completed Porting Complication Categories");

            Console.WriteLine("Started Porting Symptom Categories");
            await dataPorting.PortSymptomCategories();
            Console.WriteLine("Completed Porting Symptom Categories");

            Console.WriteLine("Started Porting Sign Categories");
            await dataPorting.PortSignCategories();
            Console.WriteLine("Completed Porting Sign Categories");

            Console.WriteLine("Started Porting Test Categories");
            await dataPorting.PortTestCategories();
            Console.WriteLine("Completed Porting Test Categories");

            //// sync level 3
            Console.WriteLine("Started Porting Dept Group Priorities");
            await dataPorting.PortDeptGroupPriorities();
            Console.WriteLine("Completed Porting Dept Group Priorities");
            Console.WriteLine("Started Porting Shortcuts ");
            await dataPorting.PortShortcuts();
            Console.WriteLine("Completed Porting Shortcuts ");
            Console.WriteLine("Started Porting Patients");
            await dataPorting.PortPatients();
            Console.WriteLine("Completed Porting Patients");

            Console.WriteLine("Started Porting Patient Tags");
            await dataPorting.PortPatientTags();
            Console.WriteLine("Completed Porting Patient Tags");

            Console.WriteLine("Started Porting Form Formats");
            await dataPorting.PortFormFormats();
            Console.WriteLine("Completed Porting Form Formats");

            Console.WriteLine("Started Porting Medicines");
            await dataPorting.PortMedicines();
            Console.WriteLine("Completed Porting Medicines");

            Console.WriteLine("Started Porting Complication Master");
            await dataPorting.PortComplicationMaster();
            Console.WriteLine("Completed Porting Complication Master");

            Console.WriteLine("Started Porting Symptoms Master");
            await dataPorting.PortSymptomsMaster();
            Console.WriteLine("Completed Porting Symptoms Master");

            Console.WriteLine("Started Porting Sign Master");
            await dataPorting.PortSignsMaster();
            Console.WriteLine("Completed Porting Sign Master");

            // // Sync Level 4
            // PatientPhotos do not exist in accessdb - hence no porting

            Console.WriteLine("Started Porting Prescriptions");
            await dataPorting.PortPrescriptions();
            Console.WriteLine("Completed Porting Prescriptions");

            Console.WriteLine("Started Porting Prescription Comments");
            await dataPorting.PortPrescriptionComments();
            Console.WriteLine("Completed Porting Prescription Comments");

            Console.WriteLine("Started Porting Events");
            await dataPorting.PortEvents();
            Console.WriteLine("Completed Porting Events");

            Console.WriteLine("Started Porting Illnesses");
            await dataPorting.PortIllnesses();
            Console.WriteLine("Completed Porting Illnesses");

            Console.WriteLine("Started Porting Dept_Test_Priorities");
            await dataPorting.PortDept_Test_Priorities();
            Console.WriteLine("Completed Porting Dept_Test_Priorities");

            Console.WriteLine("Started Porting Category_Test_Priorities");
            await dataPorting.PortCategory_Test_Priorities();
            Console.WriteLine("Completed Porting Category_Test_Priorities");

            Console.WriteLine("Started Porting Complications");
            await dataPorting.PortComplications();
            Console.WriteLine("Completed Porting Complications");

            Console.WriteLine("Started Porting Symptoms");
            await dataPorting.PortSymptoms();
            Console.WriteLine("Completed Porting Symptoms");

            Console.WriteLine("Started Porting Signs");
            await dataPorting.PortSigns();
            Console.WriteLine("Completed Porting Signs");

            Console.WriteLine("Started Porting Forms");
            await dataPorting.PortForms();
            Console.WriteLine("Completed Porting Forms");

            Console.WriteLine("Started Porting Investigations");
            await dataPorting.PortInvestigations();
            Console.WriteLine("Completed Porting Investigations");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");
        }

        private static async void PortSyncLevel0()
        {
            DataPorting dataPorting = new DataPorting();
            // sync level 0 
            Console.WriteLine("Started Porting Locations");
            await dataPorting.PortLocations();
            Console.WriteLine("Completed Porting Locations");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");
        }
        private static async void PortSyncLevel1()
        {
            DataPorting dataPorting = new DataPorting();

            ////sync level 1
            Console.WriteLine("Started Porting Roles");
            await dataPorting.PortRoles();
            Console.WriteLine("Completed Porting Roles");
            Console.WriteLine("Started Porting Users ");
            await dataPorting.PortUsers();
            Console.WriteLine("Completed Porting Users ");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");
        }
        private static async void PortSyncLevel2()
        {
            DataPorting dataPorting = new DataPorting();

            // To be able to run only from sync level 2, we need the adminuserid
            Console.WriteLine("Fetching required details for sync level 2");
            await dataPorting.GetDetailsRequiredForSyncLevel2();

            // sync level 2
            Console.WriteLine("Started Porting UsersInRoles");
            await dataPorting.PortUsersInRoles();
            Console.WriteLine("Completed Porting UsersInRoles");
            Console.WriteLine("Started Porting Groups");
            await dataPorting.PortMedicineGroups();
            Console.WriteLine("Completed Porting Groups");
            Console.WriteLine("Started Porting Departments");
            await dataPorting.PortPatientDepartments();
            Console.WriteLine("Completed Porting Departments");
            Console.WriteLine("Started Porting DataEntryTypes");
            await dataPorting.PortShortcutDataEntryTypes();
            Console.WriteLine("Completed Porting DataEntryTypes");
            Console.WriteLine("Started Porting ShortcutKeys");
            await dataPorting.PortShortcutKeys();
            Console.WriteLine("Completed Porting ShortcutKeys");

            Console.WriteLine("Started Porting Complication Categories");
            await dataPorting.PortComplicationCategories();
            Console.WriteLine("Completed Porting Complication Categories");

            Console.WriteLine("Started Porting Symptom Categories");
            await dataPorting.PortSymptomCategories();
            Console.WriteLine("Completed Porting Symptom Categories");

            Console.WriteLine("Started Porting Sign Categories");
            await dataPorting.PortSignCategories();
            Console.WriteLine("Completed Porting Sign Categories");

            Console.WriteLine("Started Porting Test Categories");
            await dataPorting.PortTestCategories();
            Console.WriteLine("Completed Porting Test Categories");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");
        }
        private static async void PortSyncLevel3()
        {
            DataPorting dataPorting = new DataPorting();

            // To be able to run only from sync level 3
            Console.WriteLine("Fetching required details for sync level 3");
            await dataPorting.GetDetailsRequiredForSyncLevel3();

            // sync level 3
            Console.WriteLine("Started Porting Dept Group Priorities");
            await dataPorting.PortDeptGroupPriorities();
            Console.WriteLine("Completed Porting Dept Group Priorities");

            Console.WriteLine("Started Porting Shortcuts ");
            await dataPorting.PortShortcuts();
            Console.WriteLine("Completed Porting Shortcuts ");

            Console.WriteLine("Started Porting Patients");
            await dataPorting.PortPatients();
            Console.WriteLine("Completed Porting Patients");

            Console.WriteLine("Started Porting Patient Tags");
            await dataPorting.PortPatientTags();
            Console.WriteLine("Completed Porting Patient Tags");

            Console.WriteLine("Started Porting Form Formats");
            await dataPorting.PortFormFormats();
            Console.WriteLine("Completed Porting Form Formats");

            Console.WriteLine("Started Porting Medicines");
            await dataPorting.PortMedicines();
            Console.WriteLine("Completed Porting Medicines");

            Console.WriteLine("Started Porting Complication Master");
            await dataPorting.PortComplicationMaster();
            Console.WriteLine("Completed Porting Complication Master");

            Console.WriteLine("Started Porting Symptoms Master");
            await dataPorting.PortSymptomsMaster();
            Console.WriteLine("Completed Porting Symptoms Master");

            Console.WriteLine("Started Porting Sign Master");
            await dataPorting.PortSignsMaster();
            Console.WriteLine("Completed Porting Sign Master");

            Console.WriteLine("Started Porting Investigation Master");
            await dataPorting.PortInvestigationsMaster();
            Console.WriteLine("Completed Porting Investigation Master");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");
        }
        private static async void PortSyncLevel4()
        {
            DataPorting dataPorting = new DataPorting();

            // To be able to run only from sync level 4
            Console.WriteLine("Fetching required details for sync level 4");
            await dataPorting.GetDetailsRequiredForSyncLevel4();

            //// // Sync Level 4
            //// PatientPhotos do not exist in accessdb - hence no porting

            //Console.WriteLine("Started Porting Events");
            await dataPorting.PortEvents();
            Console.WriteLine("Completed Porting Events");

            //// Optional
            Console.WriteLine("Started checking for discrepencies in data");
            await dataPorting.CheckEventsValidity();
            Console.WriteLine("Completed checking for discrepencies in data");



            Console.WriteLine("Started Porting Illnesses");
            await dataPorting.PortIllnesses();
            Console.WriteLine("Completed Porting Illnesses");

            Console.WriteLine("Started Porting Dept_Test_Priorities");
            await dataPorting.PortDept_Test_Priorities();
            Console.WriteLine("Completed Porting Dept_Test_Priorities");

            Console.WriteLine("Started Porting Category_Test_Priorities");
            await dataPorting.PortCategory_Test_Priorities();
            Console.WriteLine("Completed Porting Category_Test_Priorities");

            Console.WriteLine("Started Porting Prescription Comments");
            await dataPorting.PortPrescriptionComments();
            Console.WriteLine("Completed Porting Prescription Comments");

            Console.WriteLine("Started Porting Complications");
            await dataPorting.PortComplications();
            Console.WriteLine("Completed Porting Complications");


            Console.WriteLine("Started Porting Symptoms");
            await dataPorting.PortSymptoms();
            Console.WriteLine("Completed Porting Symptoms");

            Console.WriteLine("Started Porting Signs");
            await dataPorting.PortSigns();
            Console.WriteLine("Completed Porting Signs");

            Console.WriteLine("Started Porting Forms");
            await dataPorting.PortForms();
            Console.WriteLine("Completed Porting Forms");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");

            Console.WriteLine("Started Porting Prescriptions");
            await dataPorting.PortPrescriptions();
            Console.WriteLine("Completed Porting Prescriptions");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");

            Console.WriteLine("Started checking for discrepencies in data");
            await dataPorting.CheckPrescriptionsValidity();
            Console.WriteLine("Completed checking for discrepencies in data");

            Console.WriteLine("Started Porting Investigations");
            await dataPorting.PortInvestigations();
            Console.WriteLine("Completed Porting Investigations");

            Console.WriteLine("Started Writing Exception records to files");
            await dataPorting.WriteChangedOrExceptionRecordsToFile();
            Console.WriteLine("Completed Writing Exception records to files");

          //  Optional
            Console.WriteLine("Started checking for discrepencies in data");
            await dataPorting.CheckInvestigationsValidity();
            Console.WriteLine("Completed checking for discrepencies in data");


        }

        static void Main(string[] args)
        {
            try
            {
                // Program.PortAllData();  // Note - preferrably do sync by levels. ie. first run sync level 0 only, then sync level 1 only etc. 
                Program.PortSyncLevel0();
                Program.PortSyncLevel1();
                Program.PortSyncLevel2();
                Program.PortSyncLevel3();
                Program.PortSyncLevel4();

                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
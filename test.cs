using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Web;
using System.Linq;
using System.Globalization;

using BatchLibrary.App;
using AsusLibrary;
using AsusLibrary.Config;
using AsusLibrary.Entity;
using AsusLibrary.Map;
using AsusLibrary.Logic;
using AsusLibrary.Utility;

using System.Data;
using System.Text.RegularExpressions;
using Asus.Data;
using Asus.Bussiness.Map;

namespace BatchLibrary
{
    /// <summary>
    /// ISN Parser Batch
    /// </summary>

    public class ISNParser : BaseApp
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(ISNParser));

        private string mailMessage = "";

        private string errormailMessage = "";

        private string allRoundMessage = "";

        private string errorColor = "red";

        private string errorSize = "3";

        string file = "";

        string[] RunFlag;

        //20110415 Add Urgent RunFlag
        string[] URunFlag;

        private string mErrfilename = "";

        private string mSrcPath = "";

        private string mDesPath = "";

        private string mErrPath = "";

        private int limit = 1000;

        private List<string> needPalletID = new List<string>();

        public ISNParser()
            : base()
        { }

        public override int DoJob(string[] args)
        {
            //throw new Exception("test disconnected");
            //***********************************************************
            EnumStatus status = EnumStatus.none;
            //try
            //{
            log.Info("開始取得SN file");

            string sql2 = @"select CLASS2 BU from SCM.B2B_DICT where PROJECTID='GProduct' and CLASS1='checkPalletID'";
            DataTable dt2 = DBConnect(sql2, "LOGDB");
            for (int i = 0; i < dt2.Rows.Count; i++)
                needPalletID.Add(dt2.Rows[i]["BU"].ToString());

            List<CompanyPathEntity> maplist = LoginInfo.CompFolderList;

            foreach (CompanyPathEntity t in maplist)
            {
                //20110415 Add Urgent Process Folder
                //開始轉檔時會產生一個Flag檔, 如果轉檔時間超過批次執行間隔時間, 程式就可檢查flag檔來判斷該不該執行
                
                //20141023 Add limit param
                if (args.Length > 2)
                {
                    try
                    {
                        limit = Int32.Parse(args[2]);
                    }
                    catch { }
                }

                //20141023 Add param for each EMS company
                if (args.Length > 3)
                {
                    if (args[3] != "")
                    {
                        t.SourcePath = t.SourcePath + "\\" + args[3];
                        t.SrcZipPath = t.SourcePath + "\\Zip";
                    }
                }


                URunFlag = Directory.GetFiles(t.SourcePath + "\\Urgent", "LOCKFILE");

                if (URunFlag.Length == 0)
                {
                    mSrcPath = t.SourcePath + "\\Urgent";
                    mDesPath = t.DestPath;
                    mErrPath = t.ErrPath;

                    FileInfo firc = new FileInfo(t.SourcePath + "\\Urgent\\LOCKFILE");
                    StreamWriter sw = firc.CreateText();
                    sw.WriteLine(DateTime.Now.ToString());
                    sw.Flush();
                    sw.Close();

                    string[] filelist = Directory.GetFiles(t.SourcePath + "\\Urgent", "SFIS-ISN*.xml");

                    string sql, LogName;
                    //FileInfo zipfile;

                    log.Info("Start to Get Urgent File. Total Urgent File : " + filelist.Length);

                    if (filelist.Length > 0)
                    {
                        bool isSuccess;
                        foreach (string fn in filelist)
                        {
                            mailMessage = "";

                            LogName = new FileInfo(fn).Name;
                            //zipfile = null;
                            try
                            {
                                sql = @"select EXT1 from SCM.B2B_TRANSACTION_LOG where NAME = 'ISN XML' and upper(EXT1) like upper('{0}%') and rowNum = 1";
                                sql = string.Format(sql, Regex.Replace(LogName, ".xml", ".", RegexOptions.IgnoreCase));
                                DataTable dt = DBConnect(sql, "LOGDB");
                                if (dt.Rows.Count == 1)
                                {
                                    LogName = dt.Rows[0][0].ToString();
                                    setB2BLog("ISN Parser Start", LogName, "NA", "NA", "NA", "NA");
                                    //zipfile = new FileInfo(t.SrcZipPath + "\\" + LogName);
                                }
                                else
                                {
                                    setB2BLog("ISN XML", "FTP", "Inbound", "ISN Parser Start", LogName, "NA", "NA", "NA", "NA");
                                }
                            }
                            catch { }

                            //errormailMessage = "";
                            try
                            {
                                isSuccess = ParseFile(fn);
                                if (!isSuccess)
                                {
                                    //20151202 失敗訊息統一寫回DB 不再發信通知
                                    //20151224 先保留寫DB & MAIL 同時進行
                                    mailMessage += "<br>";
                                    SendMail(mailMessage, false, mErrPath + "\\" + file);
                                    status = EnumStatus.fail;
                                    try
                                    {
                                        setB2BLog("ISN Parser Error", LogName, "NA", "NA", "NA", "NA");
                                        //if (zipfile != null && zipfile.Exists)
                                        //    MoveFile(isSuccess, isSuccess, zipfile.FullName, mDesPath, mErrPath);
                                    }
                                    catch { }
                                }
                                else
                                {
                                    try
                                    {
                                        setB2BLog("ISN Parser End", LogName, "NA", "NA", "NA", "NA");
                                        //if (zipfile != null && zipfile.Exists)
                                        //    MoveFile(isSuccess, isSuccess, zipfile.FullName, mDesPath, mErrPath);
                                    }
                                    catch { }
                                }
                            }
                            catch (Exception ex)
                            {
                                log.Info("程式失敗：" + ex.Message + ex.StackTrace);

                                //mailMessage += "Program Error =" + ex.Message + ex.StackTrace+"<br>";
                                mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Program Error =" + ex.Message + ex.StackTrace + "</font><br>";

                                //errormailMessage += "Program Error =" + ex.Message + ex.StackTrace + "<br>";
                                errormailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Program Error =" + ex.Message + ex.StackTrace + "</font><br>";

                                MoveFile(true, false, mErrfilename, mDesPath, mErrPath);

                                SendMail(errormailMessage, false, mErrPath + "\\" + file);

                                status = EnumStatus.fail;

                                try
                                {
                                    setB2BLog("ISN Parser Error", LogName, "NA", "NA", "NA", "NA");
                                    //if (zipfile != null && zipfile.Exists)
                                    //    MoveFile(false, false, zipfile.FullName, mDesPath, mErrPath);
                                }
                                catch { }
                            }
                            allRoundMessage += mailMessage;
                        }
                        //mailMessage += "Urgent Job Finish<br>";
                        //SendMail(mailMessage, true, null);
                        allRoundMessage = "<font color='red' size='5'>Urgent Job Finish</font><br><br>" + allRoundMessage;
                        SendMail(allRoundMessage, true, null);
                        allRoundMessage = "";
                    }
                    else
                    {
                        log.Info("No Urgent File need to do Parser");
                    }
                    firc.Delete();
                }


                //開始轉檔時會產生一個Flag檔, 如果轉檔時間超過批次執行間隔時間, 程式就可檢查flag檔來判斷該不該執行
                //如果Flag檔產生超過三小時, 目前來說並不正常, 程式就會自動將Flag檔刪除, 以便下次Job可以執行
                RunFlag = Directory.GetFiles(t.SourcePath, "LOCKFILE");

                if (RunFlag.Length == 0)
                {
                    mSrcPath = t.SourcePath;
                    mDesPath = t.DestPath;
                    mErrPath = t.ErrPath;

                    FileInfo firc = new FileInfo(t.SourcePath + "\\LOCKFILE");
                    StreamWriter sw = firc.CreateText();
                    sw.WriteLine(DateTime.Now.ToString());
                    sw.Flush();
                    sw.Close();

                    string[] filelist = Directory.GetFiles(t.SourcePath, "SFIS-ISN*.xml");

                    string[] ziplist = Directory.GetFiles(t.SrcZipPath, "SFIS-ISN*.xml");

                    List<string> list = new List<string>();
                    list.AddRange(filelist);
                    list.AddRange(ziplist);

                    //20141023 add limit each time
                    list.Sort(
                        delegate(string fn1, string fn2){
                            string format = "yyyyMMddHHmmssfff";
                            FileInfo f1 = new FileInfo(fn1);
                            FileInfo f2 = new FileInfo(fn2);
                            //return Comparer<string>.Default.Compare(f1.LastWriteTime.ToString(format), f2.LastWriteTime.ToString(format));
                            return Comparer<string>.Default.Compare(f1.CreationTime.ToString(format), f2.CreationTime.ToString(format));
                        });
                    if (list.Count > limit)
                    {
                        list.RemoveRange(limit, list.Count - limit);
                    }

                    filelist = list.ToArray();

                    FileInfo LogName;
                    //FileInfo zipfile;

                    log.Info("Start to Get File. Total File : " + filelist.Length);

                    if (filelist.Length > 0)
                    {
                        bool isSuccess;
                        Monitor SN_Monitor = new Monitor();
                        SN_Monitor.open();
                        SN_Monitor.update_time("SFIS-ISN");
                        int count_monitor = 0;

                        foreach (string fn in filelist)
                        {
                            mailMessage = "";
                            //errormailMessage = "";
                            count_monitor += count_monitor;

                            //zipfile = null;
                            LogName = new FileInfo(fn);
                            if (fn.IndexOf(t.SrcZipPath) >= 0)
                            {
                                //zipfile = new FileInfo(Regex.Replace(fn, ".xml", ".zip", RegexOptions.IgnoreCase));
                                LogName = new FileInfo(Regex.Replace(fn, ".xml", ".zip", RegexOptions.IgnoreCase));
                            }

                            try
                            {
                                try
                                {
                                    setB2BLog("ISN Parser Start", LogName.Name, "NA", "NA", "NA", "NA");
                                }
                                catch (Exception exc)
                                {
                                    log.Info("程式失敗：" + exc.Message + exc.StackTrace);
                                }
                                isSuccess = ParseFile(fn);
                                if (!isSuccess)
                                {
                                    //20151202 失敗訊息統一寫回DB 不再發信通知
                                    //20151224 先保留寫DB & MAIL 同時進行
                                    mailMessage += "<br>";
                                    SendMail(mailMessage, false, mErrPath + "\\" + file);
                                    status = EnumStatus.fail;
                                    try
                                    {
                                        setB2BLog("ISN Parser Error", LogName.Name, "NA", "NA", "NA", "NA");
                                        //if (zipfile != null && zipfile.Exists)
                                        //    MoveFile(isSuccess, isSuccess, zipfile.FullName, mDesPath, mErrPath);
                                    }
                                    catch { }
                                }
                                else
                                {
                                    try
                                    {
                                        setB2BLog("ISN Parser End", LogName.Name, "NA", "NA", "NA", "NA");
                                        //if (zipfile != null && zipfile.Exists)
                                        //    MoveFile(isSuccess, isSuccess, zipfile.FullName, mDesPath, mErrPath);
                                    }
                                    catch { }
                                }
                                if (count_monitor == 10)
                                {
                                    SN_Monitor.update_time("SFIS-ISN");
                                    count_monitor = 0;
                                }
                            }
                            catch (Exception ex)
                            {
                                /*20160204 未處理的例外狀況寄信時只寄錯誤訊息，詳細追蹤紀錄只留下txt log*/

                                log.Info("程式失敗：" + ex.Message + ex.StackTrace);

                                //mailMessage += "Program Error =" + ex.Message + ex.StackTrace+"<br>";
                                mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Program Error =" + ex.Message /*+ ex.StackTrace*/ + "</font><br>";

                                //errormailMessage += "Program Error =" + ex.Message + ex.StackTrace + "<br>";
                                errormailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Program Error =" + ex.Message /*+ ex.StackTrace*/ + "</font><br>";

                                MoveFile(true, false, mErrfilename, mDesPath, mErrPath);

                                SendMail(errormailMessage, false, mErrPath + "\\" + file);

                                status = EnumStatus.fail;

                                try
                                {
                                    setB2BLog("ISN Parser Error", LogName.Name, "NA", "NA", "NA", "NA");
                                    //if (zipfile != null && zipfile.Exists)
                                    //    MoveFile(false, false, zipfile.FullName, mDesPath, mErrPath);
                                }
                                catch { }
                            }
                            allRoundMessage += mailMessage;
                        }
                        //mailMessage += "Job Finish<br>";
                        //SendMail(mailMessage, true, null);
                        allRoundMessage = "<font color='red' size='5'>Normal Job Finish</font><br><br>" + allRoundMessage;
                        SendMail(allRoundMessage, true, null);
                        SN_Monitor.close();
                    }
                    else
                    {
                        log.Info("No File need to do Parser");
                    }
                    firc.Delete();
                }
                else
                {
                    FileInfo firc = new FileInfo(t.SourcePath + "\\LOCKFILE");
                    if (firc.CreationTime < DateTime.Now.AddHours(-3))
                    {
                        firc.Delete();
                    }
                }

            }

            log.Info("Job Finish");

            status = EnumStatus.success;
            return 1;
        }

        public bool ParseFile(string fn)
        {
            string filename = "";
            bool isSuccess = false;
            bool isMove = false;

            filename = fn;

            mErrfilename = fn;

            file = GetFileName(fn);

            log.Info("FileName=" + filename);

            mailMessage += String.Format("<br>Start to Parsing, Filename is {0} <br>", fn);

            errormailMessage = String.Format("<br>Start to Parsing, Filename is {0} <br>", fn);

            isSuccess = ISNRequestService(fn);

            if (isSuccess)
            {
                isMove = true;

            }
            else
            {
                isMove = false;
            }

            MoveFile(isSuccess, isMove, filename, mDesPath, mErrPath);
            return isSuccess;
        }

        private string GetFileName(string fileFullPathName)
        {
            FileInfo fi = new FileInfo(fileFullPathName);

            return fi.Name;
        }

        public void MoveFile(bool isSuccess, bool isMove, string filename, string destFolder, string errFolder)
        {
            FileInfo pi = new FileInfo(filename);

            string destfullpath = destFolder + "\\" + pi.Name;
            string errFilePath = errFolder + "\\" + pi.Name;


            try
            {
                if (isMove)
                {


                    if (File.Exists(destfullpath))
                    {
                        File.Delete(destfullpath);
                    }


                    pi.MoveTo(destfullpath);

                    log.Info("Move File to Done Folder");
                    mailMessage += "Move file to DONE folder<br><br>";
                }
                else
                {

                    if (File.Exists(errFilePath))
                    {
                        File.Delete(errFilePath);
                    }

                    pi.MoveTo(errFilePath);

                    log.Info("Move File to Error Folder");
                    mailMessage += "Move file to ERROR folder<br><br>";
                    errormailMessage += "Move file to ERROR folder<br><br>";
                }
            }
            catch (System.IO.IOException ex)
            {
                log.Info("Move File Fail");
                log.Info(ex.ToString());
            }
        }

        public bool ISNRequestService(string xmlFilename)
        {
            bool isSuccess = true;

            ISNRequestMap map = new ISNRequestMap();

            try
            {

                map = XMLUtil.XMLFileToClass<ISNRequestMap>(xmlFilename);

                mailMessage += "XMLFile-->Map Finished<br>";
                errormailMessage += "XMLFile-->Map Finished<br>";

                log.Info("XMLFile-->Map Finished");

                ISNEntity objlist = CovertToEntity(map);

                if (objlist.IsSuccess)
                {
                    mailMessage += "Map-->Entity Finished<br>";
                    errormailMessage += "Map-->Entity Finished<br>";

                    log.Info("Map-->Entity Finished");

                    if (InsertDB(objlist))
                    {
                        isSuccess = true;

                        mailMessage += "Entity-->DB Finished<br>";
                        errormailMessage += "Entity-->DB Finished<br>";

                        log.Info("Entity-->DB Finished");
                    }
                    else
                    {
                        isSuccess = false;

                        mailMessage += "Entity-->DB Failed<br>";
                        errormailMessage += "Entity-->DB Failed<br>";

                        log.Info("Entity-->DB Failed");
                    }
                }
                else
                {
                    isSuccess = false;

                    log.Info("Map-->Entity Failed");

                    mailMessage += "Map-->Entity Failed<br>";
                    errormailMessage += "Map-->Entity Failed<br>";
                }

            }
            catch (InvalidOperationException ex)
            {
                List<Command> error_cmdlst = new List<Command>();

                LogError(error_cmdlst, ex.Message + "\n" + ex.InnerException.Message, file, map.MSGID ?? "");

                DbAssistant.DoCommand(error_cmdlst, DataBaseDB.ISNDB);

                isSuccess = false;

                throw new Exception(ex.Message + "\n" + ex.InnerException.Message + "\n");
            }
            catch (Exception ex)
            {
                List<Command> error_cmdlst = new List<Command>();

                LogError(error_cmdlst, ex.ToString(), file, map.MSGID ?? "");

                DbAssistant.DoCommand(error_cmdlst, DataBaseDB.ISNDB);

                isSuccess = false;

                throw ex;
            }
 
            return isSuccess;
        }

        private ISNEntity CovertToEntity(ISNRequestMap map, string filename = null)
        {
            ISNEntity isnEntity = new ISNEntity();

            //// Head 
            ISNHeadEntity obj = new ISNHeadEntity();
            List<Command> error_cmdlst = new List<Command>();

            string dt = "";
            bool isSuccess = false;
            string validMessage = Valid(map, filename);

            if (validMessage != "")
            {
                //mailMessage += "Head Error: " + validMessage + "<br>";
                mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Head Error: " + validMessage + "</font><br>";
                isnEntity.IsSuccess = false;
                isSuccess = false;

                LogError(error_cmdlst, validMessage, file, map.MSGID);

            }
            else
            {
                isSuccess = true;

                obj.JobReference = "SFIS_" + map.SENDID + "_ASUS_ISN_" + DateTime.Now.ToString("yyyyMMddHHmmssfff");
                obj.B2BRECTIME = DateTime.Now.ToString();
                obj.ISANO = map.MSGID;
                obj.MSGTYPE = map.MSGTYPE;
                obj.COPYFLAG = "N";
                obj.STNUM = 0;
                obj.ERRNUM = 0;
                obj.COMPANYID = map.SENDID;
                obj.MSGDATETIME = DateTime.ParseExact(map.MSGDATE, "yyyyMMddHHmmss", null).ToString("yyyy/MM/dd HH:mm:ss");
                obj.FILENAME = file;

                isnEntity.IsSuccess = true;
            }     

            if (isSuccess)
            {

                List<ISNMasterEntity> objlist = new List<ISNMasterEntity>();

                List<ISNDetailEntity> objlist2 = new List<ISNDetailEntity>();

                List<ISNKPDetailEntity> objlist3 = new List<ISNKPDetailEntity>();

                foreach (ISNMasterMap s in map.ItemsInfo)
                {
                    string reason_code = Valid2(s, map.MSGID);

                    if (reason_code != "")
                    {
                        error_cmdlst = LogError(error_cmdlst, reason_code, file, map.MSGID, s.DN_NO, s.SO_NO, s.PO_NO, s.ISN);

                        //item.COPYFLAG = "X";
                        //item.REASON_CODE = "B2B0001"; //chingho 必填欄未填值 
                        //item.B2B_CHECK = reason_code;
                        //errormailMessage += s.DOCID + "-" + reason_code + "<br>";


                        //20151006
                        mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>Master Error: DOC_ID='" + s.DOCID + "'\n" + reason_code + "</font><br>";
                        isnEntity.IsSuccess = false;
                        isSuccess = false;


                        continue;
                    }
                    

                    ISNMasterEntity item = new ISNMasterEntity();

                    System.Threading.Thread.Sleep(50);
                 
                    item.COPYFLAG = "N";
                    item.REASON_CODE = "";
                    item.B2B_CHECK = "";
              
                    string err = "";
                    try
                    {
                        item.JOBREF = "SFIS_" + map.SENDID + "_ASUS_ISN_" + DateTime.Now.ToString("yyyyMMddHHmmssfff");
                        item.VENDOR_TYPE = s.VENDOR_TYPE;
                        item.VENDOR_ID = s.VENDOR_ID;
                        item.DOCID = s.DOCID;
                        item.DOCDATETIME = DateTime.ParseExact(s.DOCDATETIME, "yyyyMMddHHmmss", null).ToString("yyyy/MM/dd HH:mm:ss");
                        item.ISANO = map.MSGID;
                        item.BU = s.BU;
                        item.HUB_TYPE = s.HUB_TYPE;
                        item.ISN = s.ISN;
                        item.ORI_ISN = s.ORI_ISN;
                        item.SSN = s.SSN;
                        item.ASUS_PN = s.ASUS_PN;
                        item.SO_NO = s.SO_NO;
                        item.SO_LINE = s.SO_LINE;
                        item.PO_NO = s.PO_NO;
                        item.PO_LINE = s.PO_LINE;
                        //20110218 add--begin--
                        item.DN_NO = s.DN_NO;
                        item.DN_LINE = s.DN_LINE;
                        //20110218 add--end--
                        item.ORDER_QTY = s.ORDER_QTY;
                        item.SHIP_QTY = s.SHIP_QTY;
                        item.SHIP_DATE = DateTime.ParseExact(s.SHIP_DATE, "yyyyMMdd", null).ToString("yyyy/MM/dd");
                        item.PALLET_ID = s.PALLET_ID;
                        item.PALLET_NW = s.PALLET_NW;
                        item.PALLET_GW = s.PALLET_GW;
                        item.CARTON_ID = s.CARTON_ID;
                        item.CARTON_NW = s.CARTON_NW;
                        item.CARTON_GW = s.CARTON_GW;
                        item.BOX_QTY = s.BOX_QTY;
                        item.WARRANTY = s.WARRANTY;

                        item.PALLET_NO = s.PALLET_NO;

                        //20140617 add
                        try
                        {
                            item.TRUCK_NO = s.TRUCK_NO;
                            item.LOAD_NUMBER = s.LOAD_NUMBER;
                        }
                        catch { }

                        //20150112 add
                        item.IS_PARTIAL = s.IS_PARTIAL;

                        dt = item.DOCDATETIME;// DateTime.ParseExact(s.DOCDATETIME, "yyyyMMddHHmmss", null).ToString("yyyy/MM/dd HH:mm:ss");
                        switch (s.WARR_EXP_DATE)
                        {
                            case "":
                            case null:
                                item.WARR_EXP_DATE = null;
                                break;
                            default:
                                item.WARR_EXP_DATE = DateTime.ParseExact(s.WARR_EXP_DATE, "yyyyMMdd", null).ToString("yyyy/MM/dd");
                                break;
                        }
                        item.DOA = s.DOA;
                        item.SENDID = map.SENDID;
                        item.RECEID = map.RECEID;

                        item.MSGDATE = DateTime.ParseExact(map.MSGDATE, "yyyyMMddHHmmss", null).ToString("yyyy/MM/dd HH:mm:ss");
                        item.B2BRECTIME = DateTime.Now.ToString();
                        item.DESCRIPTION = null;
                        /*----------------1*/
                        /*List<string> debug2 = new List<string>();*/
                        //---------------*/
                        if(s.ItemsInfo != null)
                            foreach (ISNDetailMap t in s.ItemsInfo)
                            {
                                string reason_code1 = Valid3(t);

                                if (reason_code1 != "")
                                {
                                    error_cmdlst = LogError(error_cmdlst, reason_code1, file, map.MSGID, s.DN_NO, s.SO_NO, s.PO_NO, s.ISN, t.ISN_DETAIL_VALUE);

                                    //item.COPYFLAG = "X";

                                    //item.REASON_CODE = "B2B0001"; //chingho 必填欄未填值

                                    //item.B2B_CHECK += reason_code1;

                                    //errormailMessage += s.DOCID + "-" + reason_code1 + "<br>";

                                    //20151006
                                    mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>ISN_DETAIL Error:"
                                                    + "<font color='blue' size='" + errorSize + "'> ISN: " + s.ISN + "  DETAIL_VALUE: " + t.ISN_DETAIL_VALUE + "</font><br>   "
                                                    + reason_code1 + "</font><br>"; isnEntity.IsSuccess = false;
                                    isSuccess = false;


                                    continue;
                                }

                                ISNDetailEntity isnd = new ISNDetailEntity();                 

                                isnd.JOBREF = item.JOBREF;
                                isnd.DOCDATETIME = item.DOCDATETIME;
                                isnd.ISN_DETAIL_TYPE = t.ISN_DETAIL_TYPE;
                                isnd.ISN_DETAIL_SEQ = t.ISN_DETAIL_SEQ;
                                isnd.ISN_DETAIL_VALUE = t.ISN_DETAIL_VALUE;
                                isnd.ISN_DETAIL_PROPERTY = t.ISN_DETAIL_PROPERTY;
                                /*----------------2*/
                                //string tmp2 = string.Format("{0},{1},{2},{3}", isnd.JOBREF, isnd.ISN_DETAIL_TYPE, isnd.ISN_DETAIL_SEQ, isnd.DOCDATETIME);
                                //if (debug2.Contains(tmp2))
                                //    throw new Exception(tmp2);
                                //else
                                //    debug2.Add(tmp2);
                                //---------------*/

                                if (t.ISN_DETAIL_DATE != "")
                                {
                                    isnd.ISN_DETAIL_DATE = DateTime.ParseExact(t.ISN_DETAIL_DATE, "yyyyMMdd", null).ToString("yyyy/MM/dd");
                                }
                                else
                                {
                                    isnd.ISN_DETAIL_DATE = null;
                                }

                                objlist2.Add(isnd);

                                if (t.ItemsInfo != null)
                                {
                                    /*----------------3*/
                                    List<string> debug = new List<string>();
                                    //---------------*/
                                    foreach (ISNKPDetailMap m in t.ItemsInfo)
                                    {
                                        string reason_code2 = Valid4(m);

                                        if (reason_code2 != "")
                                        {
                                            //item.COPYFLAG = "X";
                                            //item.REASON_CODE = "B2B0001"; //chingho 必填欄未填值
                                            //item.B2B_CHECK += reason_code2;
                                            //errormailMessage += s.DOCID + "-" + reason_code2 + "<br>";


                                            //20151006
                                            mailMessage += "<font color='" + errorColor + "' size='" + errorSize + "'>KPSN_DETAIL Error: " + reason_code2 + "</font><br>";

                                            error_cmdlst = LogError(error_cmdlst, reason_code2, file, map.MSGID, s.DN_NO, s.SO_NO, s.PO_NO, s.ISN, t.ISN_DETAIL_VALUE, m.KPSN_DETAIL_VALUE);
                                            isnEntity.IsSuccess = false;
                                            isSuccess = false;

                                            continue;
                                        }

                                        ISNKPDetailEntity iskp = new ISNKPDetailEntity();                      

                                        iskp.JOBREF = item.JOBREF;
                                        iskp.DOCDATETIME = item.DOCDATETIME;
                                        iskp.ISN_DETAIL_TYPE = t.ISN_DETAIL_TYPE;
                                        iskp.ISN_DETAIL_SEQ = t.ISN_DETAIL_SEQ;
                                        iskp.KPSN_DETAIL_TYPE = m.KPSN_DETAIL_TYPE;
                                        iskp.KPSN_DETAIL_SEQ = m.KPSN_DETAIL_SEQ;
                                        iskp.KPSN_DETAIL_VALUE = m.KPSN_DETAIL_VALUE;
                                        iskp.KPSN_DETAIL_PROPERTY = m.KPSN_DETAIL_PROPERTY;
                                        /*----------------Console.WriteLine("step3");--------4
                                        string tmp = string.Format("{0},{1},{2},{3},{4},{5}", iskp.JOBREF, iskp.DOCDATETIME, iskp.ISN_DETAIL_TYPE, iskp.ISN_DETAIL_SEQ, iskp.KPSN_DETAIL_TYPE, iskp.KPSN_DETAIL_SEQ);
                                        if (debug.Contains(tmp))
                                            throw new Exception(tmp);
                                        else
                                            debug.Add(tmp);
                                        //---------------*/
                                        if (m.KPSN_DETAIL_DATE != "")
                                        {
                                            iskp.KPSN_DETAIL_DATE = DateTime.ParseExact(m.KPSN_DETAIL_DATE, "yyyyMMdd", null).ToString("yyyy/MM/dd");
                                        }
                                        else
                                        {
                                            iskp.KPSN_DETAIL_DATE = null;
                                        }

                                        objlist3.Add(iskp);
                                    }
                                }

                            }
                    }                    
                    catch (Exception ex) {
                        /*---------------5
                        Console.WriteLine(s.DOCID);
                        Console.WriteLine(s.ISN);
                        Console.WriteLine(ex.Message);
                        Console.Read();
                        //---------------*/

                        err = err + string.Format(" Error at section which DOCID={0} and ISN={1}<br/>", s.DOCID, s.ISN);
                        
                        if (error_cmdlst.Count == 0)
                            error_cmdlst = LogError(error_cmdlst, err + " " + ex.ToString(), file, map.MSGID, s.DN_NO, s.SO_NO, s.PO_NO, s.ISN);
                                                
                        DbAssistant.DoCommand(error_cmdlst, DataBaseDB.ISNDB);
                        
                        throw new Exception(err + " " + ex.ToString());                                         
                    }
                    objlist.Add(item);
                   

                    //有問題的檢核結果才加到mail裡
                    if (item.B2B_CHECK != "")
                    {
                        mailMessage += s.DOCID + "-" + item.B2B_CHECK + "<br>";
                        //errormailMessage += s.DOCID + "-" + item.B2B_CHECK + "<br>";
                    }
           
                }

                obj.STNUM = objlist.Count;
                obj.DOCDATETIME = dt;

                isnEntity.Head = obj;
                isnEntity.MasterList = objlist;
                isnEntity.DetailList = objlist2;
                isnEntity.KPDetailList = objlist3;

                mailMessage += "ISANO is " + obj.ISANO + "<br>";
                mailMessage += "Success Count: " + obj.STNUM + "<br>";
                //mailMessage += "<font color=red>Error Count: " + obj.ERRNUM + "</font><br>";

            }

            if (error_cmdlst.Count > 0)
            {
                DbAssistant.DoCommand(error_cmdlst, DataBaseDB.ISNDB);
            }

            return isnEntity;
        }

        private string Valid(ISNRequestMap map, string fn = null)
        {
            string validMessage = "";

            ISNLogic logic = new ISNLogic();

            if ((map.MSGID == "") || (map.MSGID == null))
            {
                log.Info("MSGID為空值");

                validMessage += "MSGID is NULL<br>";
            }

            if (validMessage == "")
            {
                if (logic.IsDuplicate(map.MSGID, map.SENDID))
                {
                    FileInfo fi = new FileInfo(fn);
                    if (logic.IsDuplicate(map.MSGID, map.SENDID, fi.Name))
                    {
                        validMessage += "Old File and ";
                    }
                    else
                    {
                        validMessage += "New File but ";
                    }
                    log.Info("ISANO 重覆; Job結束");

                    validMessage += "ISANO Duplicate<br>ISANO(MSGID): " + map.MSGID + "<br>";
                }
                else
                {
                    if ((map.MSGTYPE == "") || (map.MSGTYPE == null))
                    {
                        log.Info("MSGTYPE為空值");

                        validMessage += "MSGTYPE is NULL<br>";
                    }

                    if ((map.MSGDATE == "") || (map.MSGDATE == null))
                    {
                        log.Info("MSGDATE為空值");

                        validMessage += "MSGDATE is NULL<br>";
                    }
                    else
                    {
                        if (TryDateTimeFormat(map.MSGDATE) != "")
                        {
                            validMessage += "MSGDATE='" + map.MSGDATE + "' " + TryDateTimeFormat(map.MSGDATE, "yyyyMMddHHmmss") + "<br>";

                            log.Info(validMessage);                            
                        }
                    }

                    if ((map.SENDID == "") || (map.SENDID == null))
                    {
                        log.Info("SENDID為空值");

                        validMessage += "SENDID is NULL<br>";
                    }
                    if ((map.RECEID == "") || (map.RECEID == null))
                    {
                        log.Info("RECEID為空值");

                        validMessage += "RECEID is NULL<br>";
                    }
                    if (map.ItemsInfo == null || map.ItemsInfo.Count() == 0)
                    {
                        log.Info("未包含MASTER TAG");

                        validMessage += "MASTER TAG not found<br>";
                    }

                }
            }            

            return validMessage;
        }

        private string Valid2(ISNMasterMap s, string p)
        {
            string reasonCode = "";

            if ((s.DOCID == "") || (s.DOCID == null))
            {
                log.Info("DOCID為空值");

                reasonCode += "DOCID is NULL/";
            }
            if ((s.DOCDATETIME == "") || (s.DOCDATETIME == null))
            {
                log.Info("DOCDATETIME為空值");

                reasonCode += "DOCDATETIME is NULL/";
            }
            else
            {
                if (TryDateTimeFormat(s.DOCDATETIME, "yyyyMMddHHmmss") != "")
                {
                    reasonCode += "DOCDATETIME='" + s.DOCDATETIME + "' " + TryDateTimeFormat(s.DOCDATETIME, "yyyyMMddHHmmss");
                }
            }
            if ((s.VENDOR_TYPE == "") || (s.VENDOR_TYPE == null))
            {
                log.Info("VENDOR_TYPE為空值");

                reasonCode += "VENDOR_TYPE is NULL/";
            }

            if ((s.VENDOR_ID == "") || (s.VENDOR_ID == null))
            {
                log.Info("VENDOR_ID為空值");

                reasonCode += "VENDOR_ID is NULL/";
            }
            if ((s.BU == "") || (s.BU == null))
            {
                log.Info("BU為空值");

                reasonCode += "BU is NULL/";
            }
            if (s.BU == "LCD")
            {
                if ((s.PALLET_NO == "") || (s.PALLET_NO == null))
                {
                    log.Info("Pallet_NO為空值");

                    reasonCode += "Pallet_NO is NULL/";
                }
            }
            if (needPalletID.Contains(s.BU.ToUpper()))
            {
                if ((s.PALLET_ID == "") || (s.PALLET_ID == null))
                {
                    log.Info("Pallet_ID為空值");

                    reasonCode += "Pallet_ID is NULL/";
                }             
            }
            if ((s.ISN == "") || (s.ISN == null))
            {
                log.Info("ISN為空值");

                reasonCode += "ISN is NULL/";
            }
            if ((s.ORI_ISN == "") || (s.ORI_ISN == null))
            {
                log.Info("ORI_ISN為空值");

                reasonCode += "ORI_ISN is NULL/";
            }
            if ((s.ASUS_PN == "") || (s.ASUS_PN == null))
            {
                log.Info("ASUS_PN為空值");

                reasonCode += "ASUS_PN is NULL/";
            }


            //20111114 add--begin--
            if (s.SO_NO == null)
            {
                s.SO_NO = "";
            }                      //20120101要刪掉
            if (s.SO_LINE == null)
            {
                s.SO_LINE = "";
            }                      //20120101要刪掉
            if (s.PO_NO == null)
            {
                s.PO_NO = "";
            }                      //20120101要刪掉
            if (s.PO_LINE == null)
            {
                s.PO_LINE = "";
            }                      //20120101要刪掉
            if (s.DN_NO == null)
            {
                s.DN_NO = "";
            }                      //20120101要刪掉
            if (s.DN_LINE == null)
            {
                s.DN_LINE = "";
            }                      //20120101要刪掉

            int check;

            if (s.SO_NO != "" && s.SO_NO.Length >= 4)
            {
                if (s.SO_NO.Substring(3, 1) == "-" && s.SO_LINE != "" && s.PO_NO == "" && s.PO_LINE == "" && s.DN_NO == "" && s.DN_LINE == "")
                {
                    check = 0;//SO舊單
                }
                else
                {
                    check = 1;
                }
            }
            else if (s.PO_NO != "" && s.PO_NO.Length >= 4)
            {
                if (s.PO_NO.Substring(3, 1) == "-" && s.PO_LINE != "" && s.SO_NO == "" && s.SO_LINE == "" && s.DN_NO == "" && s.DN_LINE == "")
                {
                    check = 0;//PO舊單
                }
                else
                {
                    check = 1;
                }
            }
            else
            {
                check = 1;
            }
            Single result;
            string[] SpecialCodeEMS = { "B1", "B2", "B3", "B4" };//Brasil
            if (Single.TryParse(s.DN_NO, out result) && s.DN_LINE != "" && s.SO_NO == "" && s.SO_LINE == "" && s.PO_NO == "" && s.PO_LINE == "")
            { }//判斷DN為一連串數字
            else if (check == 0)
            { }//判斷SO,PO第四字元為"-"及卡vendor_ID='BZ-EMS-VA'
            // 20121212 Add
            else if (s.SO_NO != "" && s.SO_LINE != "" && s.PO_NO == "" && s.PO_LINE == "" && s.DN_NO == "" && s.DN_LINE == "")
            { }//判斷SO有值
            else if (s.SO_NO == "" && s.SO_LINE == "" && s.PO_NO != "" && s.PO_LINE != "" && s.DN_NO == "" && s.DN_LINE == "")
            { }//判斷PO有值
            /* 20121212 Mark
            else if (s.DOA == "Y" && ((s.SO_NO != "" && s.SO_LINE != "") || (s.PO_NO != "" && s.PO_LINE != "" ))&& s.DN_NO == "" && s.DN_LINE == "")
            {}//判斷DOA為"Y"
            else if (Array.IndexOf(SpecialCodeEMS, s.SSN.Substring(4, 2)) != -1 && s.SO_NO != "" && s.SO_LINE != "" && s.PO_NO == "" && s.PO_LINE == "" && s.DN_NO == "" && s.DN_LINE == "")
            {}//判斷SSN第5.6字元是否為EMS特殊代碼以及SO有值
            else if (Array.IndexOf(SpecialCodeEMS, s.SSN.Substring(4, 2)) != -1 && s.PO_NO != "" && s.PO_LINE != "" && s.SO_NO == "" && s.SO_LINE == "" && s.DN_NO == "" && s.DN_LINE == "")
            {}//判斷SSN第5.6字元是否為EMS特殊代碼以及PO有值
            */
            else
            {
                log.Info("格式有誤");
                reasonCode += "格式有誤/";

                if (s.DN_NO != "")
                {
                    if (s.DN_NO.IndexOf("-") == -1)
                    {
                        if (s.SO_NO != "" || s.PO_NO != "" || s.SO_LINE != "" || s.PO_LINE != "")
                        {
                            log.Info("新單請填寫DN 不需填寫SO or PO");
                            reasonCode += "新單請填寫DN 不需填寫SO or PO/";
                        }

                        if (s.DN_LINE == "")
                        {
                            log.Info("有DN_NO沒有DN_Line");
                            reasonCode += "有DN_NO沒有DN_Line/";
                        }
                    }
                    else
                    {
                        log.Info("此非新單格式 請重新確認");
                        reasonCode += "此非新單格式 請重新確認/";
                    }
                }
                else
                {
                    if ((s.SO_NO != "" && s.PO_NO != "") || (s.SO_NO == "" && s.PO_NO == ""))
                    {
                        log.Info("SO及PO同時出現或無值");
                        reasonCode += "SO及PO同時出現或無值/";
                    }
                    else
                    {
                        log.Info("此舊單格式有誤 請重新確認");         //舊單錯誤點判斷式可放這
                        reasonCode += "此舊單格式有誤 請重新確認/";
                    }

                    if (s.SO_NO != "" && (s.SO_LINE == "" || s.PO_LINE != ""))
                    {
                        log.Info("有SO_NO 但SO_LINE or PO_LINE有誤");
                        reasonCode += "有SO_NO 但SO_LINE or PO_LINE有誤/";
                    }
                    if (s.PO_NO != "" && (s.PO_LINE == "" || s.SO_LINE != ""))
                    {
                        log.Info("有PO_NO 但SO_LINE or PO_LINE有誤");
                        reasonCode += "有PO_NO 但SO_LINE or PO_LINE有誤/";
                    }
                }
            }


            //20111101 add--end--

            if ((s.ORDER_QTY == "") || (s.ORDER_QTY == null))
            {
                log.Info("ORDER_QTY為空值");

                reasonCode += "QRDER_QTY is NULL/";
            }
            if ((s.SHIP_QTY == "") || (s.SHIP_QTY == null))
            {
                log.Info("SHIP_QTY為空值");

                reasonCode += "SHIP_QTY is NULL/";
            }
            if ((s.SHIP_DATE == "") || (s.SHIP_DATE == null))
            {
                log.Info("SHIP_DATE為空值");

                reasonCode += "SHIP_DATE is NULL/";
            }
            else
            {
                if (TryDateTimeFormat(s.SHIP_DATE, "yyyyMMdd") != "")
                {                    
                    string er = "SHIP_DATE='" + s.SHIP_DATE + "' " + TryDateTimeFormat(s.SHIP_DATE, "yyyyMMdd");

                    log.Info(er);
                    reasonCode += er;
                }
            }

            if (s.ItemsInfo == null)
            {
                log.Info("ISN DETAIL TAG NOT FOUND");

                reasonCode += "ISN DETAIL TAG NOT FOUND/";
            }

            if (s.WARR_EXP_DATE != null && s.WARR_EXP_DATE != "")
            {
                if (TryDateTimeFormat(s.WARR_EXP_DATE, "yyyyMMdd") != "")
                {
                    string er = "WARR_EXP_DATE='" + s.WARR_EXP_DATE + "' " + TryDateTimeFormat(s.WARR_EXP_DATE, "yyyyMMdd");

                    log.Info(er);
                    reasonCode += er;
                }
            }


            return reasonCode;
        }

        private string Valid3(ISNDetailMap t)
        {
            string reasonCode1 = "";

            if ((t.ISN_DETAIL_TYPE == null) || (t.ISN_DETAIL_TYPE == ""))
            {
                log.Info("ISN_DETAIL_TYPE為空值");

                reasonCode1 += "SN_DETAIL_TYPE is NULL/";
            }
            if ((t.ISN_DETAIL_SEQ == null) || (t.ISN_DETAIL_SEQ == ""))
            {
                log.Info("ISN_DETAIL_SEQ為空值");

                reasonCode1 += "ISN_DETAIL_SEQ is NULL/";
            }
            if ((t.ISN_DETAIL_TYPE == "PCBA" || t.ISN_DETAIL_TYPE == "ITEM80" || t.ISN_DETAIL_TYPE == "BIOS") && t.ISN_DETAIL_SEQ != "1")
            {
                log.Info("一筆SN至多各一筆(PCBA, ITEM80, BIOS)");
                reasonCode1 += "一筆SN至多各一筆(PCBA, ITEM80, BIOS)/";
            }
            if ((t.ISN_DETAIL_VALUE == null) || (t.ISN_DETAIL_VALUE == ""))
            {
                log.Info("ISN_DETAIL_VALUE為空值");

                reasonCode1 += "ISN_DETAIL_VALUE is NULL/";
            }
            if ((t.ISN_DETAIL_TYPE == "KPSN") && ((t.ISN_DETAIL_PROPERTY == "") || (t.ISN_DETAIL_PROPERTY == null)))
            {
                log.Info("ISN_DETAIL_PROPERTY為空值");

                reasonCode1 += "ISN_DETAIL_PROPERTY is NULL/";
            }

            //if (t.ItemsInfo == null)
            //{
            //    log.Info("KPSN DETAIL TAG NOT FOUND");

            //    reasonCode1 += "KPSN DETAIL TAG NOT FOUND<br>";
            //}

            if (t.ISN_DETAIL_DATE != "" && t.ISN_DETAIL_DATE != null)
            {
                if (TryDateTimeFormat(t.ISN_DETAIL_DATE, "yyyyMMdd") != "")
                {
                    string er = "ISN_DETAIL_DATE='" + t.ISN_DETAIL_DATE + "' " + TryDateTimeFormat(t.ISN_DETAIL_DATE, "yyyyMMdd");

                    log.Info(er);
                    reasonCode1 += er;
                }
            }
      

            if(t.ISN_DETAIL_TYPE == "KPSN")
            {
                if (t.ItemsInfo == null)
                    t.ItemsInfo = new ISNKPDetailMap [] {};

                if (t.ItemsInfo.Where(x => x.KPSN_DETAIL_TYPE == "KPPN").Count() == 0 ||
                    t.ItemsInfo.Where(x => x.KPSN_DETAIL_TYPE == "KPACT").Count() == 0 ||
                    t.ItemsInfo.Where(x => x.KPSN_DETAIL_TYPE == "INTIME").Count() == 0
                    )
                {
                    log.Info("KPSN 必須含有 'KPPN'、'KPACT'、'INTIME' 三種KPSN");

                    reasonCode1 += "When ISN_DETAIL_TYPE == 'KPSN'，KPSN_DETAIL 'KPPN'、'KPACT'、'INTIME' is required/";
                }
            }


            return reasonCode1;
        }

        private string Valid4(ISNKPDetailMap m)
        {
            string reasonCode2 = "";

            if ((m.KPSN_DETAIL_TYPE == null) || (m.KPSN_DETAIL_TYPE == ""))
            {
                log.Info("KPSN_DETAIL_TYPE為空值");

                reasonCode2 += "KPSN_DETAIL_TYPE is NULL/";
            }
            if ((m.KPSN_DETAIL_SEQ == null) || (m.KPSN_DETAIL_SEQ == ""))
            {
                log.Info("KPSN_DETAIL_SEQ為空值");

                reasonCode2 += "KPSN_DETAIL_SEQ is NULL/";
            }
            if ((m.KPSN_DETAIL_VALUE == null) || (m.KPSN_DETAIL_VALUE == ""))
            {
                log.Info("KPSN_DETAIL_VALUE為空值");

                reasonCode2 += "KPSN_DETAIL_VALUE is NULL/";
            }

            if (m.KPSN_DETAIL_DATE != "" && m.KPSN_DETAIL_DATE != null)
            {
                if (TryDateTimeFormat(m.KPSN_DETAIL_DATE, "yyyyMMdd") != "")
                {
                    string er = "KPSN_DETAIL_DATE='" + m.KPSN_DETAIL_DATE + "' " + TryDateTimeFormat(m.KPSN_DETAIL_DATE, "yyyyMMdd");

                    log.Info(er);
                    reasonCode2 += er;
                }
            } 

            return reasonCode2;
        }

        private string TryDateTimeFormat(string dateString)
        {

            return TryDateTimeFormat(dateString, "yyyyMMddHHmmss");
        }

        private string TryDateTimeFormat(string dateString, string dateFormat)
        {
            DateTime dt2;

            string validMessage = "";

            try
            {
                dt2 = DateTime.ParseExact(dateString, dateFormat, null, DateTimeStyles.AdjustToUniversal);
            }
            catch (Exception ex1)
            {
                validMessage += String.Format("DATETIME Format Error {0} <br>", ex1.Message);
            }

            return validMessage;
        }

        private bool InsertDB(ISNEntity obj)
        {
            string mailMessage = "";

            ISNLogic logic = new ISNLogic();

            if (logic.InsertISN(obj))
            {
                log.Info("Entity-->DB Done");

                mailMessage += "Entity-->DB Done<br>";

                return true;
            }
            else
            {
                log.Info("Entity-->DB Failed，資料無法新增");

                mailMessage += "Entity-->DB Failed<br>";

                return false;
            }
        }

        private List<Command> LogError(List<Command> plst ,string ErrorMsg, string pFileName, string pISANO = "", string pDN = "", string pSO = "", string pPO = "", string pISN = "", string pD_value = "", string pKP_Value = "")
        {
            Command cmd = new Command() { CommandText = @"Insert into ASUSB2B.EMS_ISN_ERROR_LOG
                                                           (FILENAME, ISANO, DN, SO, PO, ISN, D_VALUE, KP_VALUE, ERRORMSG)
                                                         Values
                                                           (:FileName, :ISANO, :DN, :SO, :PO, :ISN, :D_VALUE, :KP_VALUE, :ERRORMSG)"
            };

            cmd.Parameters.Add(new Parameter() { ParameterName = ":FileName", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pFileName ?? ""});
            cmd.Parameters.Add(new Parameter() { ParameterName = ":ISANO", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pISANO ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":DN", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pDN ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":SO", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pSO ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":PO", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pPO ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":ISN", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pISN ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":D_VALUE", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pD_value ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":KP_VALUE", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = pKP_Value ?? "" });
            cmd.Parameters.Add(new Parameter() { ParameterName = ":ERRORMSG", DataType = DataType.VarChar, Direction = ParameterDirection.Input, Value = ErrorMsg });

            plst.Add(cmd);

            return plst;
        }

        private void SendMail(string bodyMessage, bool isMove, string fileName)
        {
//            return;

            string mailServer = LoginInfo.MailServer;

            int port = LoginInfo.MailPort;

            string from = LoginInfo.MailFrom;

            string toList = "";

            string subject = "";

            int i = 0;

            if (fileName != null)
            {
                ISNLogic logic = new ISNLogic();
                DataTable mailUserList = logic.geterrMaillist(fileName.Substring(fileName.IndexOf("SFIS-ISN_") + 9, fileName.IndexOf("_", fileName.IndexOf("SFIS-ISN_") + 9) - (fileName.IndexOf("SFIS-ISN_") + 9)));

                if (mailUserList.Rows.Count > 0)
                {
                    i += 1;
                    foreach (DataRow row in mailUserList.Rows)
                    {
                        if (i == mailUserList.Rows.Count)
                        {
                            toList += row["Mail_Address"].ToString();
                        }
                        else
                        {
                            toList += row["Mail_Address"].ToString() + ",";
                            i += 1;
                        }
                    }
                }

                subject = String.Format("ASUS Received a Fail ISN File at {0}, Please Check and Resend.", DateTime.Now.ToString());
            }
            else
            {
                ISNLogic logic = new ISNLogic();
                DataTable mailUserList = logic.geterrMaillist("ASUS");
                toList = mailUserList.Rows[0]["Mail_Address"].ToString();

                subject = String.Format("B2B-ISN File Status at {0}", DateTime.Now.ToString());
            }

            string body = "";

            body = "Dear All:";

            string title = String.Format(" System send this mail to inform this Info. Current Time: {0}", DateTime.Now.ToString());

            body += String.Format("<p>{0}</p>", title);

            body += bodyMessage;

            if (!isMove)
            {
                try
                {
                    MailUtil.SendBodyHtml(mailServer, port, from, toList, subject, body, true, new string[] { fileName }, null);
                }
                catch
                {
                    MailUtil.SendBodyHtml(mailServer, port, from, toList, subject, body, true, new string[] { "The_XML_file_is_too_large_to_send.txt" }, null);
                }
            }
            else
            {
                MailUtil.SendBodyHtml(mailServer, port, from, toList, subject, body, true, null);
            }
        }

        public void setB2BLog(string m_status, string m_ext1, string m_ext2, string m_ext3, string m_ext4, string m_ext5)
        {
            List<DbConnStringMap> list = LoginInfo.SystemDbList;
            foreach (DbConnStringMap map in list)
            {
                Asus.Data.Configuration.DataFarm.AddConnection(map.ConnectionName, map.ConnectionString, map.DataBaseType);
            }
            System.Data.OleDb.OleDbConnection oleConn = null;
            string spName = "b2b_infra_log.transaction_log('" + m_status + "','" + m_ext1 + "','" + m_ext2 + "','" + m_ext3 + "','" + m_ext4 + "','" + m_ext5 + "')";
            string test = DbAssistant.DataForm.DefaultConnection;
            string connString = DbAssistant.DataForm.GetConnection("LOGDB").ConnectionString;
            try
            {
                string connectionString = connString;
                oleConn = new System.Data.OleDb.OleDbConnection(connectionString);
                System.Data.OleDb.OleDbCommand oleComm;
                oleComm = new System.Data.OleDb.OleDbCommand(spName, oleConn);
                oleComm.CommandType = CommandType.StoredProcedure;
                oleConn.Open();
                object insResult = oleComm.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                oleConn.Close();
            }
        }

        public static void setB2BLog(string m_name, string m_method, string m_type, string m_status, string m_ext1, string m_ext2, string m_ext3, string m_ext4, string m_ext5)
        {
            List<DbConnStringMap> list = LoginInfo.SystemDbList;
            foreach (DbConnStringMap map in list)
            {
                Asus.Data.Configuration.DataFarm.AddConnection(map.ConnectionName, map.ConnectionString, map.DataBaseType);
            }
            System.Data.OleDb.OleDbConnection oleConn = null;
            string spName = "b2b_infra_log.transaction_log('" + m_name + "','" + m_method + "','" + m_type + "','" + m_status + @"',
                      '" + m_ext1 + "','" + m_ext2 + "','" + m_ext3 + "','" + m_ext4 + "','" + m_ext5 + "')";
            string test = DbAssistant.DataForm.DefaultConnection;
            string connString = DbAssistant.DataForm.GetConnection("LOGDB").ConnectionString;
            try
            {
                string connectionString = connString;
                oleConn = new System.Data.OleDb.OleDbConnection(connectionString);
                System.Data.OleDb.OleDbCommand oleComm;
                oleComm = new System.Data.OleDb.OleDbCommand(spName, oleConn);
                oleComm.CommandType = CommandType.StoredProcedure;
                oleConn.Open();
                object insResult = oleComm.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                oleConn.Close();
            }
        }

        protected DataTable DBConnect(String sql, String connKey)
        {
            List<DbConnStringMap> list = LoginInfo.SystemDbList;
            DataTable dt;
            foreach (DbConnStringMap map in list)
            {
                Asus.Data.Configuration.DataFarm.AddConnection(map.ConnectionName, map.ConnectionString, map.DataBaseType);
            }
            try
            {
                dt = DbAssistant.DoSelect(sql, connKey);
                return dt;
            }
            catch (Exception exc)
            {
                throw new Exception("資料庫錯誤：" + exc.ToString());
            }
        }
    }
}

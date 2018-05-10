$DATAPATH = "C:\QlikDataFiles\anab-1072"
$DATAFILE = "KPI*.xlsx"
cd "C:\QlikDataFiles\anab-1072"
python appendDailyReport.py
Add-PSSnapin Microsoft.Exchange.Management.Powershell.Admin -erroraction silentlyContinue
$username = "AKIAIDIJKQ2GSQAJ3TBQ"
$password = cat C:\QlikDataFiles\gear_kpi\mysecurestring.txt | convertto-securestring -AsPlainText -Force
$cred = new-object -typename System.Management.Automation.PSCredential `
         -argumentlist $username, $password
Send-MailMessage -From notifications@samsungknox.com -To mi.zhang@samsung.com,arthur.l@samsung.com,m.strecker@partner.samsung.com,kevin.royal@samsung.com,d.abankwah@partner.samsung.com,jim.brown@samsung.com,jihwan53.lee@samsung.com,nancy.yu@samsung.com -Subject "Canada Daily KPI Membership by card name" -Attachment ".\Membership by card name.xlsx" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
Send-MailMessage -From notifications@samsungknox.com -To mi.zhang@samsung.com,arthur.l@samsung.com,m.strecker@partner.samsung.com,kevin.royal@samsung.com,d.abankwah@partner.samsung.com,jim.brown@samsung.com,jihwan53.lee@samsung.com,nancy.yu@samsung.com -Subject "Canada Daily KPI by card name" -Attachment ".\KPI by card name.xlsx" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
Send-MailMessage -From notifications@samsungknox.com -To mi.zhang@samsung.com,arthur.l@samsung.com,m.strecker@partner.samsung.com,kevin.royal@samsung.com,d.abankwah@partner.samsung.com,jim.brown@samsung.com,jihwan53.lee@samsung.com,nancy.yu@samsung.com -Subject "Canada Daily KPI by device model" -Attachment ".\KPI by device model.xlsx" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
Send-MailMessage -From notifications@samsungknox.com -To mi.zhang@samsung.com,arthur.l@samsung.com,m.strecker@partner.samsung.com,kevin.royal@samsung.com,d.abankwah@partner.samsung.com,jim.brown@samsung.com,jihwan53.lee@samsung.com,nancy.yu@samsung.com -Subject "Canada Daily KPI by app version" -Attachment ".\KPI by app version.xlsx" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
Send-MailMessage -From notifications@samsungknox.com -To mi.zhang@samsung.com,arthur.l@samsung.com,m.strecker@partner.samsung.com,kevin.royal@samsung.com,d.abankwah@partner.samsung.com,jim.brown@samsung.com,jihwan53.lee@samsung.com,nancy.yu@samsung.com,eunice09.kim@samsung.com -Subject "Canada Cumulative Daily report" -Attachment ".\cumulative_daily_report.xlsx" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
cd "C:\users\artlee\Documents"
exit
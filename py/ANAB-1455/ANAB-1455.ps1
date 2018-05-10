$PYTHONPATH = "C:\python27"
$PYTHONHOME = "C:\python27"
$DATAPATH = "C:\QlikDataFiles\ANAB-1455"
#$CurrentDate = Get-Date -format "yyyy-M-d"
#  $CurrentDate = $CurrentDate.ToString('yyyy-mm-dd')
#   write-host $CurrentDate
$DATAFILE = "Dream_KPI_breakdown.csv"
  Write-host $DATAFILE
$username = "AKIAIDIJKQ2GSQAJ3TBQ"
$password = cat C:\QlikDataFiles\gear_kpi\mysecurestring.txt | convertto-securestring -AsPlainText -Force
$cred = new-object -typename System.Management.Automation.PSCredential `
         -argumentlist $username, $password
Add-PSSnapin Microsoft.Exchange.Management.Powershell.Admin -erroraction silentlyContinue
cd $DATAPATH
python.exe DreamKPI.py
#Send-MailMessage -From notifications@samsungknox.com -To arthur.l@samsung.com -Subject "Dream KPI" -Attachments $DATAFILE -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
Send-MailMessage -From notifications@samsungknox.com -To arthur.l@samsung.com,rui.chen1@samsung.com,c.moparthi@partner.samsung.com,a.seshadri@samsung.com,jifan.zhu@samsung.com,yonghui.c@samsung.com,mi.zhang@samsung.com,na.wang1@samsung.com,bdsilva@samsung.com,k.kartikeya@samsung.com,nancy.yu@samsung.com -Subject "Dream KPI" -Attachments $DATAFILE -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
cd "C:\users\artlee\Documents"
exit

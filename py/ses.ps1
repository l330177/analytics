Add-PSSnapin Microsoft.Exchange.Management.Powershell.Admin -erroraction silentlyContinue
$DATAPATH = "C:\QlikDataFiles\gear_kpi"
$DATAFILE = "Gear KPI.csv"
$username = "AKIAIDIJKQ2GSQAJ3TBQ"
$password = cat C:\QlikDataFiles\gear_kpi\mysecurestring.txt | convertto-securestring -AsPlainText -Force
$cred = new-object -typename System.Management.Automation.PSCredential `
         -argumentlist $username, $password
Send-MailMessage -From notifications@samsungknox.com -To arthur.l@samsung.com,e.cha@samsung.com,nancy.yu@samsung.com,a.seshadri@samsung.com,bdsilva@samsung.com -Subject "Gear KPI" -Attachments "C:\QlikDataFiles\gear_kpi\Gear KPI.csv" -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
cd "C:\users\artlee\Documents"
exit
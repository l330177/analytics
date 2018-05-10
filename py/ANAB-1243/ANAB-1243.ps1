$DATAPATH = "C:\QlikDataFiles\ANAB-1243"
#$CurrentDate = (Get-Date -f yyyy-MM-dd)
$DATAFILE = "InApp.csv"
#  # Write-host $DATAFILE
$username = "AKIAIDIJKQ2GSQAJ3TBQ"
$password = cat C:\QlikDataFiles\gear_kpi\mysecurestring.txt | convertto-securestring -AsPlainText -Force
$cred = new-object -typename System.Management.Automation.PSCredential `
         -argumentlist $username, $password
Add-PSSnapin Microsoft.Exchange.Management.Powershell.Admin -erroraction silentlyContinue
cd $DATAPATH
python inapp.py
Send-MailMessage -From notifications@samsungknox.com -To arthur.l@samsung.com -Subject "inapp" -Attachments $DATAFILE -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
#Send-MailMessage -From notifications@samsungknox.com -To arthur.l@samsung.com,vishal.katyal@looppaysamsung.com,bdsilva@samsung.com,sujung.hong@samsung.com,nancy.yu@samsung.com,ibraheem.khadar@samsungpay.com,melissa.webb@samsungpay.com,andrew.pelehach@samsungpay.com,j.gupta@samsung.com,j.chaudry@samsung.com,v.pagano@samsung.com,k.roy2@samsung.com,holly.ross@samsungpay.com,alan.m@samsung.com,priscilla.yang@samsungpay.com,meena.kaushal@partner.samsungpay.com -Subject "inapp" -Attachments $DATAFILE -SmtpServer email-smtp.us-east-1.amazonaws.com -UseSsl -Credential $cred
cd "C:\users\artlee\Documents"
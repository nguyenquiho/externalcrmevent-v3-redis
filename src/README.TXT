1. Tao link java --> javacrmevent de de dang monitor process

ln -nsf /usr/bin/java /usr/bin/javacrmevent

2. Copy vao /var/externalcrmevent
3. Chuyen thuoc tinh cho cac script:
   chmod +x /var/externalcrmevent/fdswap
   chmod +x /var/externalcrmevent/*.sh
4. Copy file externalcrmeventd vao /etc/init.d
   cp /var/externalcrmevent/externalcrmeventd /etc/init.d/externalcrmeventd
   chmod +x /etc/init.d/externalcrmeventd
   chkconfig externalcrmeventd on
5. HOW TO RUN AS Service:

   5.1 start:
   service externalcrmeventd start
   5.2 stop:
   service externalcrmeventd stop 
   
   5.3 check status
   service externalcrmeventd status
   ps -ef | grep -v grep | grep com.worldfonecc.ami.ExternalCRMService
6. HOW TO RUN from command

    5.1 start:
    /var/externalcrmevent/externalcrmeventstart.sh
   5.2 stop:
    /var/externalcrmevent/externalcrmeventstop.sh
   5.3 check status
    ps -ef | grep -v grep | grep com.worldfonecc.ami.ExternalCRMService
   
7. DEBUG:
  7.1 Set log2console=true (trong PBX.properties) , logallevents=true OR false
  7.2 Restart lai service
  7.3 Xem log trong file /var/externalcrmevent/ExternalCRMService.output.log
  7.3b Neu muon watch log
   watch -n 1 'tail /var/externalcrmevent/ExternalCRMService.output.log'

8. Log rotate, dùng để dọn file log để nó không bị quá lớn
 
  8.1 Chuyen thuoc tinh cho cac script:
   chmod +x /var/externalcrmevent/fdswap
   chmod +x /var/externalcrmevent/*.sh
  8.2 Thêm 1 task sau vào crontab
  0 * * * * /var/externalcrmevent/rotatelog.sh


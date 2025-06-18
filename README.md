# Tutorial

## run file

./bin/spark-submit  
--master k8s://https://source-code-master:6443  
--deploy-mode cluster  
--name q1dfudf.py  
--conf spark.hadoop.fs.permissions.umask-mode=000  
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  
--conf spark.kubernetes.namespace=randreou-priv  
--conf spark.executor.instances=1  
--conf spark.kubernetes.container.image=apache/spark  
--conf spark.eventLog.enabled=true  
--conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/randreou/logs  
--conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/randreou/logs  
hdfs://hdfs-namenode:9000/user/randreou/code/query1_df_udf.py

./bin/spark-submit  
--master k8s://https://source-code-master:6443  
--deploy-mode cluster  
--name q1-df-udf  
--conf spark.hadoop.fs.permissions.umask-mode=000  
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  
--conf spark.kubernetes.namespace=randreou-priv  
--conf spark.executor.instances=5  
--conf spark.kubernetes.container.image=apache/spark  
--conf spark.eventLog.enabled=true  
--conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/randreou/logs  
--conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/randreou/logs  
hdfs://hdfs-namenode:9000/user/randreou/code/query1_df_udf.py

Οπου query1_df_udf όνομα του αρχείου (Σε καποιες περιπτωσεις ισως χρειαστεί να αλλάξουμε τα instances του executor ανάλογα πόσα δεδομένα έχουμε)

## put file inside hdfs

hdfs dfs -put -f /home/rafail/project-2025/convert_to_parquet.py /user/randreou/code  
hdfs dfs -mkdir -p /user/randreou/data/parquet/ (φτιάχνω folder)

## If openvpn doesnt work

1. Close openvpn from taskmanager
2. windows + r = services.msc openvpn interactive service start
3. open wsl +docker if this still does
4. docker-compose up --build -d (inside 01..)

# Οδηγίες Εργασίας

- [x] Να ολοκληρωθεί η διαδικασία σύνδεσης με την απομακρυσμένη υποδομή kubernetes που περιγράφεται στους οδηγούς του μαθήματος. Επίσης, να ολοκληρωθεί η διαδικασία παραμετροποίησης του Spark Job History Server μέσω docker και docker compose τοπικά, στο μηχάνημά σας, ώστε να αντλεί τα δεδομένα από τις εκτελέσεις σας στην απομακρυσμένη υποδομή (HDFS). (5%)
- [x] Να γραφτεί κώδικας που θα διαβάσει τα αρχεία δεδομένων και με την κατάλληλη επεξεργασία θα τα αποθηκεύσει σε μορφή parquet στο HDFS, στο παρακάτω path: hdfs://hdfs-namenode:9000/user/{username}/data/parquet/ 10 Δώστε ιδιαίτερη προσοχή στο αρχείο που αντιστοιχεί στο dataset yellow_tripdata_2024, καθώς δε βρίσκεται σε εξαρχής σε μορφή που το Spark μπορεί να αναγνωρίσει. (5%)
- [x] Να υλοποιηθεί το Query 1 χρησιμοποιώντας τα RDD και DataFrame APIs (με udf και χωρίς). Σχολιάστε τις διαφορές στην επίδοση μεταξύ των διαφορετικών υλοποιήσεών σας. (10%)
- [x]  Να υλοποιηθεί το Query 2 χρησιμοποιώντας τα RDD, DataFrame και SQL APIs. Σχολιάστε τις επιδόσεις των υλοποιήσεών σας. (10%)
- [x] Να υλοποιηθεί το Query 3 χρησιμοποιώντας α) τo DataFrame API και β) το SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση.
- [x] Να υλοποιηθεί το Query 4 χρησιμοποιώντας τo SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση. (15%)
- [x] Να υλοποιηθεί το Query 5 χρησιμοποιώντας τo DataFrame API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση. (15%)
- [x] Να υλοποιηθεί το Query 6 χρησιμοποιώντας τo DataFrame API. Εφαρμόστε οριζόντια και κάθετη κλιμάκωση (horizontal and vertical scaling) των πόρων που δεσμεύετε για την εκτέλεση χρησιμοποιώντας τα κατάλληλα spark configurations (spark.executor.instances, spark.executor.cores, spark.executor.memory). Καλείστε να εκτελέσετε την υλοποίησή σας χρησιμοποιώντας συνολικούς πόρους 8 cores και 16GB μνήμης με τα παρακάτω configurations:  2 executors X 4 cores/8GB memory  4 executors X 2 cores/4GB memory  8 executors X 1 core/2 GB memory Σχολιάστε τα αποτελέσματα.
- [x] Για κάθε ένα από τα joins των υλοποιήσεων των Query 3, 4 και Query 5 να αναφερθεί η επιλογή στρατηγικής (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL κλπ.) που κάνει ο Catalyst Optimizer του Spark με χρήση της εντολής explain ή του Job History Server (να συμπεριληφθεί το σχετικό output ή screenshot). Να σχολιάσετε βάσει θεωρίας αν η επιλογή δικαιολογείται ή όχι βάσει των χαρακτηριστικών του join. (15%)
- [x] Ανεβάζω στο github όλους τους κώδικες που εχώ υλοποιήση στο github οπως και πιθανά scripts/howtos για την εκτέλεση του κώδικα μου(Πριν τα ανεβάσω τα ανεβάζω ξανα στο hdfs
- [x] Φτιάχνω ένα pdf με όνομα το αμ_αμχαρη το οποιο θα περιέχει μία αναφορά (αυστηρά με όσα ζητούνται στην εκφώνηση) η οποία θα περιέχει αποκλειστικά τις απαντήσεις στα ζητούμενα, καθώς και το link στο github

# Queries

## Query 1

Για κάθε ώρα της ημέρας (00 έως 23), υπολογίστε τη μέση τιμή των γεωγραφικών συντεταγμένων (πλάτος και μήκος) σημείου επιβίβασης, αγνοώντας εγγραφές με μηδενικές συντεταγμένες. Ταξινομήστε το αποτέλεσμα αύξουσα ως προς την ώρα. Χρησιμοποιήστε το 2015 dataset Ενδεικτικά αποτελέσματα: HourOfDay Latitude Longitude 00 -73,... 40,... 01 -73,... 40,...

Να υλοποιηθεί το Query 1 χρησιμοποιώντας τα RDD και DataFrame APIs (με udf και χωρίς). Σχολιάστε τις διαφορές στην επίδοση μεταξύ των διαφορετικών υλοποιήσεών σας. (10%)

## Query 2

Για κάθε Vendor (πάροχο ταξί), υπολογίστε τη μέγιστη απόσταση διαδρομής που πραγματοποιήθηκε, μετρώντας την απόσταση ως γεωδαισιακή απόσταση Haversine μεταξύ του σημείου επιβίβασης και του σημείου αποβίβασης. Χρησιμοποιήστε το 2015 dataset Επιπλέον, επιστρέψτε και τη χρονική διάρκεια της διαδρομής στην οποία παρατηρήθηκε η μέγιστη απόσταση. 7 VendorID Max Haversine Distance (km) Duration (min) 1 38.72 47.3 2 42.15 53.8 6 34.90 41.5 7 39.35 46.2

Να υλοποιηθεί το Query 2 χρησιμοποιώντας τα RDD, DataFrame και SQL APIs. Σχολιάστε τις επιδόσεις των υλοποιήσεών σας. (10%)

Για το Q2, η ταχύτητα μιας κούρσας ορίζεται ως η απόσταση που διανύθηκε προς τον χρόνο που χρειάστηκε.

## Query 3

Για κάθε δήμο (Borough) της Νέας Υόρκης, υπολογίστε τον συνολικό αριθμό διαδρομών ταξί που ξεκίνησαν και κατέληξαν στον συγκεκριμένο δήμο. Ταξινομήστε το τελικό αποτέλεσμα κατά φθίνουσα σειρά ως προς τον αριθμό των διαδρομών. Χρησιμοποιήστε το 2024 dataset Ενδεικτικά αποτελέσματα: Borough TotalTrips Manhattan 124520 Queens 78250 Brooklyn 45200 Bronx 36710

Να υλοποιηθεί το Query 3 χρησιμοποιώντας α) τo DataFrame API και β) το SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση.

## Query 4

Υπολογίστε τον συνολικό αριθμό διαδρομών ταξί που πραγματοποιήθηκαν μεταξύ 23:00 και 07:00, ανεξαρτήτως τοποθεσίας. Εξάγετε την ώρα από το πεδίο tpep_pickup_datetime και κρατήστε μόνο τις εγγραφές:  είτε από 23:00 έως 23:59  είτε από 00:00 έως 06:59 Στη συνέχεια, ομαδοποιήστε τα αποτελέσματα ανά VendorID, ώστε να φανεί πόσες νυχτερινές διαδρομές πραγματοποιήθηκαν από κάθε πάροχο. Χρησιμοποιήστε το 2024 dataset Ενδεικτικά αποτελέσματα VendorID NightTrips 1 54,820 2 47,210

Να υλοποιηθεί το Query 4 χρησιμοποιώντας τo SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση. (15%)

## Query 5

Εντοπίστε τα ζεύγη ζωνών (Zone) με τον μεγαλύτερο αριθμό μετακινήσεων μεταξύ τους, βάσει των διαδρομών ταξί. Εξαιρούνται οι περιπτώσεις όπου η διαδρομή ξεκίνησε και κατέληξε στην ίδια υπο-περιοχή. Το αποτέλεσμα πρέπει να ομαδοποιεί ανά ζεύγος PickupZone → DropoffZone και να εμφανίζει το πλήθος διαδρομών ανά ζεύγος. Ταξινομήστε φθίνουσα με βάση τον αριθμό των μετακινήσεων. Χρησιμοποιήστε το 2024 dataset Ενδεικτικά αποτελέσματα Pickup Zone Dropoff Zone TotalTrips Midtown Center Upper East Side 32140 Upper West Side Times Square 28790

Να υλοποιηθεί το Query 5 χρησιμοποιώντας τo DataFrame API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση. (15%)

## Query 6

Για κάθε δήμο (Borough) από τον οποίο ξεκίνησε μία διαδρομή ταξί, υπολογίστε τα συνολικά έσοδα από τις διαδρομές που ξεκίνησαν από αυτόν. Αναλύστε τα συνολικά έσοδα σε επιμέρους κατηγορίες χρεώσεων:  fare_amount (βασικός ναύλος),  tip_amount (φιλοδώρημα),  tolls_amount (διόδια),  extra (επιπλέον χρεώσεις),  mta_tax (φόρος MTA),  congestion_surcharge (προσαύξηση συμφόρησης),  airport_fee (χρέωση αεροδρομίου),  total_amount (συνολικό ποσό χρέωσης). Ταξινομήστε τα αποτελέσματα κατά φθίνουσα σειρά με βάση τα συνολικά έσοδα (total_amount) για να εντοπίσετε τους δήμους με τη μεγαλύτερη οικονομική δραστηριότητα. Χρησιμοποιήστε το 2024 dataset Ενδεικτικά αποτελέσματα Borou gh Fare () Tolls () MTA Tax () Airport Fee () Manh attan 1,254,32 0.50 234,189.75 112,340.00 95,81 2.40 52,13 0.00 88,390. 00 19,230.00 1,856,412.6 5 Quee ns 874,520. 30 132,450.20 98,420.00 54,21 0.75 36,74 0.00 45,210. 00 12,800.00 1,254,351.2 5 Brook lyn 432,105. 80 64,891.55 41,295.00 28,67 5.60 18,33 0.00 22,870. 00 4,310.00 612,478.95 Bronx 216,430. 25 21,840.00 12,760.00 14,38 0.90 9,200. 00 10,890. 00 1,210.00 286,711.15 Staten Isl. 42,350.0 0 3,240.00 2,180.00 1,985. 20 1,080. 00 880.00 0.00 51,715.20

Να υλοποιηθεί το Query 6 χρησιμοποιώντας τo DataFrame API. Εφαρμόστε οριζόντια και κάθετη κλιμάκωση (horizontal and vertical scaling) των πόρων που δεσμεύετε για την εκτέλεση χρησιμοποιώντας τα κατάλληλα spark configurations (spark.executor.instances, spark.executor.cores, spark.executor.memory). Καλείστε να εκτελέσετε την υλοποίησή σας χρησιμοποιώντας συνολικούς πόρους 8 cores και 16GB μνήμης με τα παρακάτω configurations:  2 executors X 4 cores/8GB memory  4 executors X 2 cores/4GB memory  8 executors X 1 core/2 GB memory Σχολιάστε τα αποτελέσματα. (15%)

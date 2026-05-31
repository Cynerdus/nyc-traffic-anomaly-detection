# Real-Time Traffic Anomaly Detection
## Presentation Script

---

# 1. Introducere — Conceptul și motivația proiectului

Scopul proiectului nostru este dezvoltarea unui sistem de detectare în timp real a anomaliilor de trafic folosind date de tip streaming provenite din cursele de taxi din New York City.

Mediile urbane moderne generează continuu cantități foarte mari de date de mobilitate. Sistemele de taxi reprezintă un exemplu foarte bun, deoarece fiecare cursă conține informații spațiale, temporale și comportamentale despre condițiile de trafic din oraș.

În loc să analizăm static date istorice offline, am dorit să simulăm un pipeline real de streaming, în care evenimentele sosesc continuu și sunt procesate aproape în timp real.

Obiectivul principal al proiectului este identificarea unor tipare anormale de trafic, precum:
- posibilă congestie,
- zone cu trafic neobișnuit de lent,
- grupuri de curse cu durată foarte mare,
- comportament instabil al traficului,
- sau creșteri bruște ale activității.

Proiectul a fost gândit ca un sistem complet end-to-end, care include:
- ingestia datelor,
- simularea streaming-ului,
- procesarea evenimentelor,
- detectarea anomaliilor,
- și vizualizarea interactivă printr-un dashboard live.

O decizie importantă de design a fost structurarea proiectului asemănător unei arhitecturi reale de streaming utilizate în sisteme moderne de tip smart city sau traffic monitoring.

---

# 2. Dataset-ul — selecție, preprocesare și motivații

Pentru dataset am utilizat NYC Taxi Trip Records, un set de date care conține informații detaliate despre cursele de taxi din New York City.

Dataset-ul original include milioane de curse și multiple tipuri de servicii, însă pentru versiunea actuală a proiectului ne-am concentrat pe:
- Yellow Taxi trips,
- și Green Taxi trips,
din ianuarie 2023.

Din dataset-ul original am extras doar atributele relevante pentru analiza traficului în timp real:
- timestamp-uri de pickup și dropoff,
- locațiile de pickup și dropoff,
- distanța cursei,
- valorile tarifelor,
- numărul de pasageri,
- și tipul serviciului.

Aceste atribute ne-au permis să derivăm metrici relevante pentru mobilitate, precum:
- durata cursei,
- viteza medie,
- densitatea traficului,
- și variabilitatea vitezei.

O provocare importantă a fost echilibrarea realismului cu constrângerile de procesare.

Inițial am experimentat cu eșantionare complet random, însă am observat că aceasta poate distorsiona comportamentul temporal al traficului și poate produce ferestre nereprezentative.

Pentru a păstra o evoluție realistă a traficului, am trecut la extragerea pe intervale de timp:
- datele Yellow Taxi au fost extrase dintr-un interval temporal mai concentrat,
- iar datele Green Taxi au necesitat un interval mai mare deoarece dataset-ul conținea mai puține curse.

Am introdus și limite maxime de sampling pentru fiecare tip de serviciu, pentru a reduce dezechilibrul dintre fluxurile Yellow și Green Taxi.

Acest lucru ne-a permis să construim un mediu experimental mai controlat, păstrând în același timp distribuții realiste ale traficului.

Dataset-ul final pregătit conține aproximativ:
- 30.000 de curse Yellow Taxi,
- și aproximativ 26.000 de curse Green Taxi.

---

# 3. Tehnologiile utilizate și componentele sistemului

Proiectul combină mai multe tehnologii, fiecare responsabilă pentru un strat diferit al pipeline-ului de streaming.

Principalele tehnologii utilizate sunt:
- Python,
- Apache Kafka,
- Streamlit,
- Pandas,
- și Plotly.

Python a fost utilizat ca limbaj principal de implementare datorită integrării foarte bune cu sisteme de streaming, biblioteci de procesare de date și framework-uri de vizualizare.

Apache Kafka reprezintă platforma centrală de event streaming.

Rolul său este:
- să primească evenimentele taxi transmise în streaming,
- să distribuie datele între componente,
- și să simuleze o arhitectură reală bazată pe evenimente.

Sistemul conține trei componente executabile majore.

Prima componentă este:
`taxi_trip_producer.py`

Acest modul:
- citește dataset-ul pregătit,
- simulează streaming-ul în timp real,
- și publică continuu cursele taxi în topic-urile Kafka.

A doua componentă este:
`windowed_anomaly_detector.py`

Aceasta reprezintă motorul principal de procesare al proiectului.

Modulul:
- consumă evenimentele taxi din Kafka,
- le grupează în ferestre temporale,
- calculează metrici de trafic,
- detectează anomalii,
- și publică rezultatele procesate înapoi în Kafka.

A treia componentă este:
`dashboard.py`

Acest modul construiește stratul de vizualizare interactivă folosind Streamlit și Plotly.

Dashboard-ul:
- consumă evenimentele de anomalie,
- agregă statistici,
- aplică filtre,
- și afișează analize și insight-uri în timp real.

Fișiere auxiliare suplimentare includ:
- `prepare_taxi_data.py` pentru preprocesare,
- `requirements.txt`,
- `docker-compose.yml`,
- și fișiere CSV de configurare și lookup.

---

# 4. Straturile de procesare și logica de detectare

Pipeline-ul de procesare urmează mai multe straturi logice.

Primul strat este stratul de ingestie.

Evenimentele taxi sunt transmise continuu în Kafka, unde fiecare cursă devine un eveniment independent care conține informații spațiale și temporale.

Al doilea strat este stratul de windowing.

În loc să procesăm cursele individual, sistemul le grupează în ferestre fixe de event time.

O fereastră reprezintă:
- un interval scurt de timp,
- pentru o anumită zonă de pickup,
- și pentru un anumit tip de serviciu taxi.

În interiorul fiecărei ferestre sunt calculați mai mulți indicatori agregați:
- viteza medie,
- durata medie,
- numărul de curse,
- raportul curselor lente,
- și variabilitatea vitezei.

Acest pas transformă evenimentele brute în indicatori relevanți pentru trafic.

Al treilea strat este detectarea anomaliilor.

Am implementat mai multe reguli bazate pe praguri.

De exemplu:
- viteza medie scăzută împreună cu multe curse lente poate indica congestie,
- vitezele extrem de mici pot indica zone cu trafic neobișnuit de lent,
- variabilitatea mare poate sugera comportament instabil al traficului,
- iar duratele foarte mari pot sugera blocaje sau mobilitate dificilă.

Fiecărei ferestre procesate îi sunt atribuite:
- un status,
- tipuri de anomalie,
- și un nivel de severitate.

În final, evenimentele procesate sunt publicate din nou în Kafka și vizualizate în dashboard.

O decizie importantă de design a fost separarea curselor Yellow și Green Taxi în ferestre independente de procesare.

Astfel, un tip de serviciu nu îl domină statistic pe celălalt în timpul agregării.

---

# 5. Interpretarea rezultatelor obținute

Rezultatele obținute au evidențiat mai multe tipare relevante de trafic.

Tipul dominant de anomalie detectat a fost:
„Possible congestion”.

Acest lucru sugerează că ferestrele de streaming conțineau frecvent:
- viteze medii scăzute,
- împreună cu proporții mari de curse lente.

Zonele identificate drept hotspot-uri de congestie au fost concentrate în special în Manhattan, în zone precum:
- East Harlem,
- Clinton East,
- Midtown,
- Gramercy,
- și East Village.

Aceste zone sunt cunoscute pentru trafic urban intens, ceea ce oferă încredere că detectorul surprinde comportamente realiste de mobilitate.

Comparația dintre serviciile Yellow și Green Taxi a evidențiat și diferențe interesante.

Ferestrele Green Taxi au prezentat:
- rate mai mari de anomalie,
- viteze medii mai mici,
- și variabilitate mai mare.

Acest lucru poate fi explicat parțial prin:
- densitate mai redusă a curselor,
- ferestre cu mai puține evenimente,
- și comportament mai volatil al traficului.

Dashboard-ul a demonstrat și că:
- filtrarea după borough,
- severitate,
- tip de serviciu,
- și categorie de anomalie
poate modifica semnificativ tiparele observate.

Acest lucru evidențiază importanța analizei exploratorii interactive în sistemele de trafic bazate pe streaming.

O altă observație importantă este că nu toate borough-urile au fost reprezentate egal în rezultate.

Acest lucru reflectă atât:
- dezechilibrele din dataset,
- cât și distribuția naturală a activității taxiurilor în NYC.

---

# 6. Îmbunătățiri și direcții viitoare

Implementarea actuală reprezintă un prototip funcțional inițial, însă există mai multe îmbunătățiri care pot crește realismul și calitatea analitică a sistemului.

Prima direcție importantă este extinderea dataset-ului pe perioade mai mari de timp.

În prezent, experimentele folosesc intervale limitate din ianuarie 2023.

Integrarea:
- mai multor luni,
- diferențelor dintre zile lucrătoare și weekend,
- perioadelor de sărbători,
- și variațiilor sezoniere
ar genera tipare de trafic mult mai reprezentative.

O altă extindere importantă este integrarea datelor meteo.

Condițiile meteorologice influențează puternic:
- viteza traficului,
- congestia,
- cererea de curse,
- și durata deplasărilor.

Prin corelarea evenimentelor meteo cu ferestrele de anomalie, sistemul ar putea identifica:
- congestie cauzată de ploaie,
- încetiniri provocate de ninsoare,
- sau perturbări de mobilitate produse de condiții extreme.

De asemenea, planificăm implementarea unui mecanism de export istoric al rezultatelor.

În prezent, evenimentele există doar în memorie în timpul rulării.

Exportarea ferestrelor procesate în fișiere CSV sau baze de date ar permite:
- analiză pe termen lung,
- studii de trend,
- validarea modelelor,
- și raportare offline.

Alte direcții viitoare pot include:
- detectare de anomalii bazată pe machine learning,
- praguri adaptive,
- predicție de congestie,
- sau vizualizări geografice integrate direct în dashboard.

Viziunea pe termen lung este transformarea prototipului într-o platformă mai realistă de monitorizare urbană capabilă să susțină analiză de mobilitate în timp real pentru medii smart city.
# CC2 – MapReduce sur `ml-25m/tags.csv`

**Auteurs :** Corentin Seuberras, Nino Mignard  
**Date :** 09/04/2026  
**Fichier source :** `ml-25m/tags.csv`  
**Format :** `userId,movieId,tag,timestamp`

---

## Environnement

- Hadoop HDP Sandbox 2.6.5 (VM VirtualBox)
- Python 2.7 / mrjob 0.7.4
- HDFS single-node (1 NameNode + 1 DataNode)

---

## Préparation

### Téléchargement des données

Le fichier `tags.csv` a été téléchargé directement depuis la VM via `wget` puis décompressé :

```bash
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
```

### Fichier échantillon pour tests locaux

Avant d'appliquer les jobs sur le fichier complet, on crée un échantillon de 1000 lignes pour valider les scripts localement sans solliciter le cluster :

```bash
head -1000 ~/ml-25m/tags.csv > ~/tags_sample.csv
```

### Installation de mrjob

mrjob n'étant pas disponible sur la VM, pip a été installé manuellement puis mrjob installé :

```bash
curl -O https://bootstrap.pypa.io/pip/2.7/get-pip.py
python get-pip.py
pip install mrjob==0.7.4
```

### Démarrage des services Hadoop

Les services HDFS et YARN n'étant pas démarrés automatiquement, ils ont été lancés manuellement :

```bash
sudo su root
ambari-agent start
ambari-server start
/usr/hdp/current/hadoop-hdfs-namenode/../hadoop/sbin/hadoop-daemon.sh start namenode
su hdfs -c "/usr/hdp/current/hadoop-hdfs-datanode/../hadoop/sbin/hadoop-daemon.sh start datanode"
hdfs dfsadmin -safemode leave
/usr/hdp/current/hadoop-yarn-resourcemanager/sbin/yarn-daemon.sh start resourcemanager
/usr/hdp/current/hadoop-yarn-nodemanager/sbin/yarn-daemon.sh start nodemanager
```

---

## Configuration par défaut de Hadoop

### Upload du fichier dans HDFS

```bash
hdfs dfs -mkdir -p /user/maria_dev/cc2/input
hdfs dfs -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input/
hdfs dfs -ls /user/maria_dev/cc2/input/
```

Résultat :
```
Found 1 items
-rw-r--r--   1 maria_dev hdfs   38810332 2026-04-09 12:49 /user/maria_dev/cc2/input/tags.csv
```

---

### Question 1 — Combien de tags chaque film possède-t-il ?

**Script `tags_per_movie.py` :**

```python
from mrjob.job import MRJob

class TagsPerMovie(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            movieId = parts[1]
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    TagsPerMovie.run()
```

**Test local :**

```bash
python tags_per_movie.py tags_sample.csv
```

**Lancement sur Hadoop :**

```bash
python tags_per_movie.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output_tags_per_movie
```

**Lecture du résultat :**

```bash
hdfs dfs -cat /user/maria_dev/cc2/output_tags_per_movie/part-* | head -30
```

**Résultat (extrait, 30 premières lignes) :**

```
"1"     697
"10"    137
"100"   18
"1000"  10
"100001"        1
"100003"        3
"100008"        9
"100017"        9
"100032"        2
"100034"        19
"100036"        1
"100038"        4
"100042"        2
"100044"        12
"100046"        3
"100048"        1
"100052"        4
"100054"        6
"100060"        10
"100062"        2
"100070"        5
"100072"        1
"100075"        16
"100077"        1
"100079"        16
"100081"        2
"100083"        87
"100087"        9
"100091"        7
"100099"        2
```

> Résultats complets (45 251 films) : [output_tags_per_movie.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_movie.txt)

**Commentaire :** Le mapper émet `(movieId, 1)` pour chaque ligne en ignorant le header. Le reducer somme les occurrences par clé `movieId`. Le job a traité 1 093 360 lignes avec 2 tâches Map et 1 tâche Reduce, produisant 45 251 entrées distinctes (45 251 films ayant au moins un tag).

---

### Question 2 — Combien de tags chaque utilisateur a-t-il ajoutés ?

**Script `tags_per_user.py` :**

```python
from mrjob.job import MRJob

class TagsPerUser(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            userId = parts[0]
            yield userId, 1
        except Exception:
            pass

    def reducer(self, userId, counts):
        yield userId, sum(counts)

if __name__ == '__main__':
    TagsPerUser.run()
```

**Test local :**

```bash
python tags_per_user.py tags_sample.csv
```

**Lancement sur Hadoop :**

```bash
python tags_per_user.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output_tags_per_user
```

**Lecture du résultat :**

```bash
hdfs dfs -cat /user/maria_dev/cc2/output_tags_per_user/part-* | head -30
```

**Résultat (extrait, 30 premières lignes) :**

```
"100001"        9
"100016"        50
"100028"        4
"100029"        1
"100033"        1
"100046"        133
"100051"        19
"100058"        5
"100065"        2
"100068"        19
"100076"        4
"100085"        3
"100087"        8
"100088"        13
"100091"        29
"100101"        3
"100125"        3
"100130"        2
"100140"        5
"100141"        26
"100155"        7
"100156"        684
"100177"        1
"100180"        2
"100189"        11
"10019" 36
"100193"        10
"100197"        1
"1002"  1
"100236"        11
```

> Résultats complets (14 592 utilisateurs) : [output_tags_per_user.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_user.txt)

**Commentaire :** Même logique que Q1, mais la clé émise par le mapper est `userId`. Le job produit 14 592 entrées distinctes, soit 14 592 utilisateurs ayant posé au moins un tag. On remarque une forte disparité : certains utilisateurs ont posé un seul tag, d'autres plusieurs centaines (ex: utilisateur 100156 avec 684 tags).

---

## Configuration Hadoop avec taille de bloc = 64 Mo

### Upload du fichier dans HDFS en config 64 Mo

```bash
hdfs dfs -mkdir -p /user/maria_dev/cc2/input_64m
hdfs dfs -D dfs.blocksize=67108864 -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input_64m/
```

---

### Question 3 — Combien de blocs le fichier occupe-t-il dans chacune des configurations ?

```bash
# Config par défaut
hdfs fsck /user/maria_dev/cc2/input/tags.csv -files -blocks

# Config 64 Mo
hdfs fsck /user/maria_dev/cc2/input_64m/tags.csv -files -blocks
```

**Résultats :**

| Configuration | Taille du bloc | Taille du fichier | Nombre de blocs |
|---|---|---|---|
| Par défaut | 128 Mo | 38,8 Mo | 1 |
| 64 Mo | 64 Mo | 38,8 Mo | 1 |

**Commentaire :** Le fichier `tags.csv` fait 38,8 Mo (38 810 332 octets). Dans les deux configurations, ce fichier est inférieur à la taille d'un bloc (128 Mo et 64 Mo), donc il occupe **1 seul bloc** dans les deux cas. Le nombre de blocs est calculé par `ceil(taille_fichier / taille_bloc)`. Pour observer plusieurs blocs, il faudrait un fichier plus volumineux, comme `ratings.csv` du même dataset qui fait ~650 Mo.

---

### Question 4 — Combien de fois chaque tag a-t-il été utilisé pour taguer un film ?

**Script `tag_frequency.py` :**

```python
from mrjob.job import MRJob

class TagFrequency(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            # Le champ tag peut contenir des virgules, on prend parts[2:-1]
            tag = ','.join(parts[2:-1]).strip().lower()
            if tag:
                yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagFrequency.run()
```

**Test local :**

```bash
python tag_frequency.py tags_sample.csv
```

**Lancement sur Hadoop :**

```bash
python tag_frequency.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64m/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output_tag_frequency
```

**Lecture du résultat :**

```bash
hdfs dfs -cat /user/maria_dev/cc2/output_tag_frequency/part-* | head -30
```

**Résultat (extrait, 30 premières lignes) :**

```
"!950's superman tv show"       1
"#1 prediction" 3
"#adventure"    1
"#antichrist"   1
"#boring #lukeiamyourfather"    1
"#boring"       1
"#danish"       2
"#documentary"  1
"#entertaining" 1
"#exorcism"     1
"#fantasy"      2
"#hanks #muchstories"   1
"#jesus"        1
"#lifelessons"  1
"#lukeiamyourfather"    1
"#metoo"        1
"#mindfulness"  1
"#notscary"     1
"#rap"  1
"#science"      1
"#scifi"        1
"#space"        1
"#thriling"     1
"#thriller #suspense #disjointed #boring"       1
"#timesup"      1
"#vatican city" 1
"#wtf"  1
"#zen"  1
"&suspense"     1
"'50's" 1
```

> Résultats complets (65 414 tags distincts) : [output_tag_frequency.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tag_frequency.txt)

**Commentaire :** Le mapper extrait le champ `tag` en gérant les éventuelles virgules dans la valeur via `','.join(parts[2:-1])`, puis normalise en minuscules. Le reducer comptabilise le total d'utilisations de chaque tag. Le job produit 65 414 tags distincts sur l'ensemble du catalogue. Le bloc `try/except` permet d'ignorer les lignes malformées sans faire échouer le job.

---

### Question 5 — Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?

**Script `tags_per_movie_user.py` :**

```python
from mrjob.job import MRJob

class TagsPerMovieUser(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            userId = parts[0]
            movieId = parts[1]
            yield '%s_%s' % (movieId, userId), 1
        except Exception:
            pass

    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    TagsPerMovieUser.run()
```

**Test local :**

```bash
python tags_per_movie_user.py tags_sample.csv
```

**Lancement sur Hadoop :**

```bash
python tags_per_movie_user.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64m/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output_tags_per_movie_user
```

**Lecture du résultat :**

```bash
hdfs dfs -cat /user/maria_dev/cc2/output_tags_per_movie_user/part-* | head -30
```

**Résultat (extrait, 30 premières lignes) :**

```
"100001_6550"   1
"100003_6550"   2
"100003_70092"  1
"100008_21096"  2
"100008_62199"  1
"100008_6550"   6
"100017_103126" 1
"100017_62199"  1
"100017_6550"   7
"100032_62199"  1
"100032_70092"  1
"100034_129101" 3
"100034_14116"  9
"100034_62199"  2
"100034_6550"   4
"100034_70092"  1
"100036_62199"  1
"100038_62199"  1
"100038_6550"   2
"100038_70092"  1
"100042_6550"   2
"100044_139646" 1
"100044_31047"  4
"100044_49134"  1
"100044_58039"  4
"100044_95673"  2
"100046_21096"  3
"100048_6550"   1
"100052_160540" 1
"100052_33844"  3
```

> Résultats complets (305 356 paires film-utilisateur) : [output_tags_per_movie_user.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_movie_user.txt)

**Commentaire :** La clé composite `movieId_userId` permet de grouper par paire (film, utilisateur). Le résultat indique, pour chaque combinaison, le nombre de tags différents qu'un même utilisateur a attribués à un même film. Le job produit 305 356 paires distinctes. Par exemple, l'utilisateur 6550 a attribué 6 tags différents au film 100008, et 7 tags au film 100017.

---

## Récapitulatif des résultats

| Question | Job | Résultats distincts | Fichier complet |
|---|---|---|---|
| Q1 | Tags par film | 45 251 films | [output_tags_per_movie.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_movie.txt) |
| Q2 | Tags par utilisateur | 14 592 utilisateurs | [output_tags_per_user.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_user.txt) |
| Q3 | Nombre de blocs | 1 bloc (les deux configs) | — |
| Q4 | Fréquence des tags | 65 414 tags distincts | [output_tag_frequency.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tag_frequency.txt) |
| Q5 | Tags par (film, utilisateur) | 305 356 paires | [output_tags_per_movie_user.txt](https://github.com/corentinseu/cc2-hadoop-mapreduce-corentinseu/blob/main/output_tags_per_movie_user.txt) |

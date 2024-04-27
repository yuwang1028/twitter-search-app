# DATA MANAGEMENT FOR ADVANCED DATA SCIENCE APPLICATIONS final project - team 15

## STAT-694 at Rutgers, year (2024)
## team 15: 
HSIAO-CHUN HUNG hh617
/ YU WANG yw1029
/ YUYUE SUN ys898
/ ZHUOER LIU zl413
## 1. Git clone the repository

```bash
git clone https://github.com/yuwang1028/twitter-search-app.git
```

<br/>

## 2. Setup python environment

### 2.a. Create a venv environment

```bash
python3 -m venv <env_name>
```

### 2.b. Source onto environment

```bash
source <env_name>/bin/activate
```

### 2.c. Install requirements.txt file using pip

```bash
pip3 install -r requirements.txt --no-cache-dir
```

<br/>

## 3. Add search API data file (or streaming data) to scripts folder

```bash
cd ./scripts
#copy data to this folder
```

## 4. Load the SQL and NoSQL databases if data is not loaded

```bash
sh scripts/setup/main.sh
```

<br/>

## 5. Setup the Front End

```bash
python3 UI/ui.py 
```

Once you run this, you should see the search app @ localhost:8000

<br/>

## 6. Setup the periodic ttl based cacher 

```bash
python3 scripts/staleCacheChecker.py 
```

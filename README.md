# DATA MANAGEMENT FOR ADVANCED DATA SCIENCE APPLICATIONS final project - team 15

## STAT-694
## team 15: HSIAO-CHUN HUNG/ YU WANG/ YUYUE SUN/ ZHUOER LIU
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

## 3. Add search API data to data folder

```bash
mkdir ./data
#copy data to this folder
```

## 3. Load the SQL and NoSQL databases if data is not loaded

```bash
sh scripts/setup/main.sh
```

<br/>

## 4. Setup the Front End

```bash
python3 UI/ui.py 
```

Once you run this, you should see the search app @ localhost:8000

<br/>

## 5. Setup the periodic ttl based cacher 

```bash
python3 scripts/staleCacheChecker.py 
```

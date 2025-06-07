# ED-colony-parser
Little python tool for reading out data from the spansh 80gb+ galaxy dump



Data is loaded initially in about 9 hours on my machine using the database_filler_copy.py variant. 
For subsequent updates, the non-copy database filler is required to avoid blowing away your data. 
# Install
```
#Have postgres installed
If using the database_filler postgres and postgis are required, and the relevant extensions must be enabled, there are several ways to do this, but if you are unfamiliar with postgres generally I recommend using stackbuilder for this at least on windows.

      Name      | Version |   Schema   |                            Description
----------------+---------+------------+-------------------------------------------------------------------
 plpgsql        | 1.0     | pg_catalog | PL/pgSQL procedural language
 postgis        | 3.5.2   | public     | PostGIS geometry and geography spatial types and functions
 postgis_raster | 3.5.2   | public     | PostGIS raster types and functions
 postgis_sfcgal | 3.5.2   | public     | PostGIS SFCGAL functions

#Have python3 installed
python3 -m pip install orjson

#clone in a bash derivitive terminal, git bash works well on windows, or your preferred flavor for linux/unix

cd <where you want to put the galaxy dump>

git clone https://github.com/MadParkerD/ED-colony-parser.git
_or_
git clone git@github.com:MadParkerD/ED-colony-parser.git
_or_
download the zip and unzip in your desired folder

# Download the spansh full galaxy dump to the same drive this program is cloned to
```



# Running: Requires the spansh 80gb+ zipped dump stored locally (do not unzip)
```
#Initial load:
python3 database_filler_copy.py <path to dump>

#Once data is loaded:
python3 database_filler.py <path to dump>

I recommend loading the stations specific dump using the supsequent run method, since thet tends to be updated more often than the whole 80gb one.
# geoclimateForStations
A combination of codes to run GeoClimate for buffer zones of different sizes around observation stations.

## Requirements
### Java and Groovy environment
You should have Java > 11 installed on your machine and a version of Groovy installed via sdk-man (cf. below).
```bash
curl -s "https://get.sdkman.io" | bash
# Follow the on-screen instructions to wrap up the installation. Afterward, open a new terminal or run the following in the same shell:
source "$HOME/.sdkman/bin/sdkman-init.sh"
#Lastly, run the following snippet to confirm the installation's success:
sdk version
```

To install a given version of Groovy, follow the following informations: https://github.com/orbisgis/geoclimate/wiki/Frequently-Asked-Questions#groovy-version-issue

### Python environment


## Get the code on your machine
### If you are a GitHub user
Open a terminal where you would like to save the code and type:
```bash
git clone git@github.com:j3r3m1/geoclimateForStations.git
```

Since geoclimateForStations is based on the geoclimatetool, you also need to download the corresponding code which has been added within this code as a submodule. Thus use the following commands:
```bash
git submodule init
git submodule update
```


# Else...
Simply download and extract the code using this download link: https://github.com/j3r3m1/geoclimateForStations/archive/refs/heads/main.zip


## Run the code
The only file that would need modifications is the Python file [Create_buffer_and_calculate.py](https://github.com/j3r3m1/geoclimateForStations/blob/26d1e6e8ce95b1fb20d07e1998947e39857ac009/Create_buffer_and_calculate.py#L27). In this file, only the lines 27 to 49 corresponding to the section "DEFINE THE VARIABLES" should be modified.


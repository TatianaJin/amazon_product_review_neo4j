# http://deepyeti.ucsd.edu/jianmo/amazon/index.html
echo please cite: Justifying recommendations using distantly-labeled reviews and fined-grained aspects
echo download review data
wget http://deepyeti.ucsd.edu/jianmo/amazon/categoryFiles/All_Amazon_Review.json.gz && gzip -d All_Amazon_Review.json.gz
echo download meta data
wget http://deepyeti.ucsd.edu/jianmo/amazon/metaFiles/All_Amazon_Meta.json.gz && gzip -d All_Amazon_Meta.json.gz

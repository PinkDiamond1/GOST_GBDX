#!/bin/sh

docker run -it -v D:\DRC_Census\Carte_RDC\FCGC600480494\:/mnt/work/input geographyis/spfeas

docker build --tag spfeas:0.4.0 -f Dockerfile .

docker login

docker tag spfeas:0.4.0 geographyis/spfeas:0.4.0
docker push geographyis/spfeas:0.4.0
docker tag spfeas:0.4.0 geographyis/spfeas:latest
docker push geographyis/spfeas:latest

#spfeas -i fullVrt.vrt -o output/ -tr orb seg dmp fourier gabor grad hog lac lbp lbpm lsr mean pantex saliency sfs --block 4 --scales 8 16 32
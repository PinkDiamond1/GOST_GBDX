library(foreign)

inFolder = "H:/MEX_Pov/UrbanAgebs"

#Open the master input dataset
#inD = read.dbf("H:/SriLanka/GN/AllGNs_Merged.dbf")
inD = read.dbf("H:/MEX_Pov/AGEBS/urban_agebs.dbf")
inD$concat_id = as.character(inD$concat_id)
origD = inD
#Get a list of input files
inFiles = list.files(inFolder, "*.csv")
tempShp = read.csv(paste(inFolder, inFiles[1], sep="/"))
#Add extra columns to inD
newCols = names(tempShp)[-which(names(tempShp) %in% names(inD))]
for (newName in newCols){
    inD[,newName] = -999
}

#Process each of the csv files individually
for (fIdx in 1:length(inFiles)) {
    curF = inFiles[fIdx]
    curD = read.csv(paste(inFolder, curF, sep="/"))
    if (nrow(curD) > 0 & curF != "missedTriggers.csv"){ #Only process the csv files with good data
        curD$concat_id = as.character(curD$concat_id)        
        for (rIdx in 1:nrow(curD)){
            curRow = curD[rIdx,]
            outRowIdx = which(inD$concat_id == curRow$concat_id)
            for (cIdx in 5:length(curRow)){
                curName = names(curD)[cIdx]
                outNameIdx = which(names(inD) == curName)
                inD[outRowIdx, outNameIdx] = curRow[cIdx]            
            }
            
        }
    }
}

inD$processed = 0
inD$processed[which(inD$orb_1_SUM > -999)] = 1
table(inD$processed)

#curStatus = inD[,c('concat_id','processed')]
#write.csv(curStatus, "H:/MEX_Pov/Results/curStatus.csv")

shpD = readOGR("H:/MEX_Pov/AGEBS","urban_agebs")
shpD@data = cbind(shpD@data, curStatus[,2])
writeOGR(shpD,"H:/MEX_Pov/AGEBS","urban_agebs_statusUpdate_2018_05_22",driver="ESRI Shapefile") 

processed = inD[which(inD$sfs_18_STD > -999),]
write.csv(processed, paste(inFolder, "urban_agebs_processed_2018_05_29.csv", sep=""))
unprocessed = inD[which(inD$sfs_18_STD == -999),]
write.csv(unprocessed, paste(inFolder, "urban_agebs_unprocessed_2018_05_29.csv", sep=""))
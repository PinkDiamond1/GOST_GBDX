inFolder = "Q:/WORKINGPROJECTS/Mexico_Poverty/spfeas_results"

inFiles = list.files(inFolder, "*.csv")
inD = read.csv(paste(inFolder, inFiles[7], sep="/"))
inD$concat_id = as.character(inD$concat_id)
origD = inD
for (fIdx in 1:5) {
    curF = inFiles[fIdx]
    curD = read.csv(paste(inFolder, curF, sep="/"))
    curD$concat_id = as.character(curD$concat_id)
    for (rIdx in 1:nrow(curD)){
        curRow = curD[rIdx,]
        outRowIdx = which(inD$concat_id == curRow$concat_id)
        for (cIdx in 11:length(curRow)){
            curName = names(curD)[cIdx]
            outNameIdx = which(names(inD) == curName)
            inD[outRowIdx, outNameIdx] = curRow[cIdx]            
        }
        
    }
}

write.csv(inD, paste(inFolder, "/updated", inFiles[7], sep=""))

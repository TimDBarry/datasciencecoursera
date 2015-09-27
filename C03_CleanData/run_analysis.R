install.packages("data.table")
install.packages("sqldf")
library(data.table)
library(utils);
library(stringr);
library(dplyr);
library(sqldf);


printnested <- function(msg) {
    nestlevel <- sys.parent() -1  #what was the nesting level.
    if (nestlevel < 0) {
        message(msg)
    } else {
        caller <- sys.call(-1)[1] #what called this proc.    
        msgfull <- str_trim(paste(caller, "; ", msg, sep="")) #concat caller to msg.
        msglen  <- nchar(msgfull)
        npad    <- (nestlevel*4) 
        spaces  <- paste(replicate(npad, " "), collapse = "")    
        msgfull <- paste(Sys.time(), "; ", spaces, msgfull, sep="")
        message(msgfull)
    }
} 


debuglvl <- 2
dir_original_wd <- getwd()


# Source-Remote location(s)
url_src_zip  <- "http://archive.ics.uci.edu/ml/machine-learning-databases/00240/UCI%20HAR%20Dataset.zip"
fil_src_zip  <- "UCI_HAR_Dataset.zip"


# Source-Local location(s)
dir_src_root <- paste(dir_original_wd, "C03_Smartphone",  sep="/")     #directory to download the .zip to.
dir_src_data <- paste(dir_original_wd, "C03_Smartphone",  sep="/")     #directory to download the .zip to.

flp_src_zip <- paste(dir_src_data, fil_src_zip, sep = "/")

# Create Source-Local directories.


# Download the sourze zip file to the Source-Local directory
if (!file.exists(flp_src_zip)) {
    download.file(url_src_zip, destfile = flp_src_zip)
}    

# Unzip the file in the Source-Locl directory.
setwd(dir_src_data)
if (debuglvl>=2) printnested(paste("Working directory:",getwd()))
if (debuglvl>=2) printnested(paste("Unzipping file :",flp_src_zip))
unzip(flp_src_zip, overwrite=TRUE)
if (debuglvl>=2) printnested(paste("Working directory:",getwd()))


#---------------------------------  Assemble Label metadata.


#Load the "Activity Labels".
fil_ActivityLabels <- paste(dir_src_data, "UCI HAR Dataset/activity_labels.txt", sep="/")
dt_ActivityLabels <- fread(fil_ActivityLabels)
colnames(dt_ActivityLabels) <- c("ActivityID","ActivityName")

#Load the "Feature" domain list to be used as column names.
fil_Features <- paste(dir_src_data, "UCI HAR Dataset/features.txt", sep="/")
dt_Features <- fread(fil_Features)
#Assign dt_X better column names using the Features data table.
colnames(dt_Features) <- c("FeatureID","FeatureName")





#---------------------------------  Assemble measure data

#--------------- Append Test to Train

#---------- X-files (Ha! Features)


#Load the Feature Training data.
fil_X <- paste(dir_src_data, "UCI HAR Dataset/train/X_train.txt", sep="/")
dt_X <- fread(fil_X)
    #, sep="auto", sep2="auto", nrows=-1L, header="auto", na.strings="NA"
    #,stringsAsFactors=FALSE, verbose=FALSE, autostart=30L, skip=-1L, select=NULL
    #,colClasses=NULL, integer64=getOption("datatable.integer64"))
# Give the Feature data the correct column names.
colnames(dt_X) <- dt_Features$FeatureName

# head(dt_X,n=3)
# str(dt_X)
# nrow(dt_X)  7352
# length(dt_X)  561


#Load the Feature Test data.
fil_X <- paste(dir_src_data, "UCI HAR Dataset/test/X_test.txt", sep="/")
dt_scratch <- fread(fil_X)
colnames(dt_scratch) <- dt_Features$FeatureName
# head(dt_scratch,n=3)
# str(dt_scratch)
# nrow(dt_scratch)  2947
# length(dt_scratch)  561


# Append the Feature Test Data to the Training Data.
bindlist = list(dt_X,dt_scratch)
dt_X <- rbindlist(bindlist, use.names=TRUE)
# nrow(dt_X)

#----- Improve Feature column names.
#Remove parentheses from the name.
names(dt_X) <- gsub("[()]", "", names(dt_X))

#Replace prefixes
names(dt_X) <- gsub("^t", "time"   , names(dt_X))
names(dt_X) <- gsub("^f", "fourier", names(dt_X))

#Make names legal.
names(dt_X) <- make.names(names(dt_X), unique = TRUE)
#Replace "std" with "stdev".
names(dt_X) <- gsub(".std", ".stdev", names(dt_X))

#----- Create a data.table of just the columns we care about.
#Create a vector of just the columns that are Means and Standard Deviations.
keepercols <- names(dt_X)[which((names(dt_X) %like% "*.mean.*") | (names(dt_X) %like% "*.stdev*"))]
dt_X2 <- dt_X[, keepercols, with=FALSE]
# nrow(dt_X2)
# str(dt_X2)
# length(dt_X2)


#---------- y-files (ActivityID)


#Load the "ActivityID" Training data.
fil_y <- paste(dir_src_data, "UCI HAR Dataset/train/y_train.txt", sep="/")
dt_y <- fread(fil_y)
colnames(dt_y)  <- c("ActivityID")

#Load the "ActivityID" Test data.
fil_y <- paste(dir_src_data, "UCI HAR Dataset/test/y_test.txt", sep="/")
dt_scratch <- fread(fil_y)
colnames(dt_scratch) <- c("ActivityID")

# Append the ActivityID Test Data to the Training Data.
bindlist = list(dt_y,dt_scratch)
dt_y <- rbindlist(bindlist, use.names=TRUE)

# Enrich this data.table by adding Activity Name.
dt_y = merge(dt_y, dt_ActivityLabels, by.x="ActivityID",by.y="ActivityID",all=FALSE)  #merge(x, y)


#---------- Subject-files


#Load the "Subject" Training data.
fil_subject <- paste(dir_src_data, "UCI HAR Dataset/train/subject_train.txt", sep="/")
dt_subject <- fread(fil_subject)
colnames(dt_subject)  <- c("SubjectID")

#Load the "Subject" Test data.
fil_subject <- paste(dir_src_data, "UCI HAR Dataset/test/subject_test.txt", sep="/")
dt_scratch <- fread(fil_subject)
colnames(dt_scratch) <- c("SubjectID")

# Append the Subject Test Data to the Training Data.
bindlist = list(dt_subject,dt_scratch)
dt_subject <- rbindlist(bindlist, use.names=TRUE)


#--------------- Join X2 -to- Y -to- Subject


# If the rowcounts match between the Features and the ActivityID, then assing a RowID to each to be used for joining.
if (nrow(dt_y)!= nrow(dt_X2) & nrow(dt_X)!=nrow(dt_subject)   ) {
    printnested(paste("Warning: Rowcounts do not match, merge() will fail."))    
    return
}


# Add RowID's to each data table to give the merge() function something to JOIN with.
dt_X2$RowID <-as.numeric(rownames(dt_X2))
setkey(dt_X2, RowID)

dt_y$RowID <-as.numeric(rownames(dt_y))
setkey(dt_y, RowID)

dt_subject$RowID <-as.numeric(rownames(dt_subject))
setkey(dt_subject, RowID)


# Merge the Subject with the Activity.
dt_subjectyX = merge(dt_subject, dt_y, by.x="RowID",by.y="RowID",all=FALSE)  #merge(x, y)
# nrow(dt_subjectyX)
# str(dt_subjectyX)            

# Now merge in the Features.
dt_subjectyX = merge(dt_subjectyX, dt_X2, by.x="RowID",by.y="RowID",all=FALSE)  #merge(x, y)
# nrow(dt_subjectyX)
# str(dt_subjectyX)            
# length(dt_subjectyX)
# table(dt_subjectyX$SubjectID, dt_subjectyX$ActivityName) #What does the distribution of data look like.




#Compute the average of each variable for each activity and each subject.

sql <- "SELECT SubjectID, ActivityName,
AVG([timeBodyAcc.mean.X]) AS [timeBodyAcc.mean.X],
AVG([timeBodyAcc.mean.Y]) AS [timeBodyAcc.mean.Y],
AVG([timeBodyAcc.mean.Z]) AS [timeBodyAcc.mean.Z],
AVG([timeBodyAcc.stdev.X]) AS [timeBodyAcc.stdev.X], 
AVG([timeBodyAcc.stdev.Y]) AS [timeBodyAcc.stdev.Y], 
AVG([timeBodyAcc.stdev.Z]) AS [timeBodyAcc.stdev.Z], 
AVG([timeGravityAcc.mean.X]) AS [timeGravityAcc.mean.X], 
AVG([timeGravityAcc.mean.Y]) AS [timeGravityAcc.mean.Y], 
AVG([timeGravityAcc.mean.Z]) AS [timeGravityAcc.mean.Z], 
AVG([timeGravityAcc.stdev.X]) AS [timeGravityAcc.stdev.X],
AVG([timeGravityAcc.stdev.Y]) AS [timeGravityAcc.stdev.Y],
AVG([timeGravityAcc.stdev.Z]) AS [timeGravityAcc.stdev.Z],
AVG([timeBodyAccJerk.mean.X]) AS [timeBodyAccJerk.mean.X],
AVG([timeBodyAccJerk.mean.Y]) AS [timeBodyAccJerk.mean.Y],
AVG([timeBodyAccJerk.mean.Z]) AS [timeBodyAccJerk.mean.Z],
AVG([timeBodyAccJerk.stdev.X]) AS [timeBodyAccJerk.stdev.X], 
AVG([timeBodyAccJerk.stdev.Y]) AS [timeBodyAccJerk.stdev.Y], 
AVG([timeBodyAccJerk.stdev.Z]) AS [timeBodyAccJerk.stdev.Z], 
AVG([timeBodyGyro.mean.X]) AS [timeBodyGyro.mean.X], 
AVG([timeBodyGyro.mean.Y]) AS [timeBodyGyro.mean.Y], 
AVG([timeBodyGyro.mean.Z]) AS [timeBodyGyro.mean.Z], 
AVG([timeBodyGyro.stdev.X]) AS [timeBodyGyro.stdev.X],
AVG([timeBodyGyro.stdev.Y]) AS [timeBodyGyro.stdev.Y],
AVG([timeBodyGyro.stdev.Z]) AS [timeBodyGyro.stdev.Z],
AVG([timeBodyGyroJerk.mean.X]) AS [timeBodyGyroJerk.mean.X], 
AVG([timeBodyGyroJerk.mean.Y]) AS [timeBodyGyroJerk.mean.Y], 
AVG([timeBodyGyroJerk.mean.Z]) AS [timeBodyGyroJerk.mean.Z], 
AVG([timeBodyGyroJerk.stdev.X]) AS [timeBodyGyroJerk.stdev.X],
AVG([timeBodyGyroJerk.stdev.Y]) AS [timeBodyGyroJerk.stdev.Y],
AVG([timeBodyGyroJerk.stdev.Z]) AS [timeBodyGyroJerk.stdev.Z],
AVG([timeBodyAccMag.mean]) AS [timeBodyAccMag.mean], 
AVG([timeBodyAccMag.stdev]) AS [timeBodyAccMag.stdev],
AVG([timeGravityAccMag.mean]) AS [timeGravityAccMag.mean],
AVG([timeGravityAccMag.stdev]) AS [timeGravityAccMag.stdev], 
AVG([timeBodyAccJerkMag.mean]) AS [timeBodyAccJerkMag.mean], 
AVG([timeBodyAccJerkMag.stdev]) AS [timeBodyAccJerkMag.stdev],
AVG([timeBodyGyroMag.mean]) AS [timeBodyGyroMag.mean],
AVG([timeBodyGyroMag.stdev]) AS [timeBodyGyroMag.stdev], 
AVG([timeBodyGyroJerkMag.mean]) AS [timeBodyGyroJerkMag.mean],
AVG([timeBodyGyroJerkMag.stdev]) AS [timeBodyGyroJerkMag.stdev], 
AVG([fourierBodyAcc.mean.X]) AS [fourierBodyAcc.mean.X], 
AVG([fourierBodyAcc.mean.Y]) AS [fourierBodyAcc.mean.Y], 
AVG([fourierBodyAcc.mean.Z]) AS [fourierBodyAcc.mean.Z], 
AVG([fourierBodyAcc.stdev.X]) AS [fourierBodyAcc.stdev.X],
AVG([fourierBodyAcc.stdev.Y]) AS [fourierBodyAcc.stdev.Y],
AVG([fourierBodyAcc.stdev.Z]) AS [fourierBodyAcc.stdev.Z],
AVG([fourierBodyAcc.meanFreq.X]) AS [fourierBodyAcc.meanFreq.X], 
AVG([fourierBodyAcc.meanFreq.Y]) AS [fourierBodyAcc.meanFreq.Y], 
AVG([fourierBodyAcc.meanFreq.Z]) AS [fourierBodyAcc.meanFreq.Z], 
AVG([fourierBodyAccJerk.mean.X]) AS [fourierBodyAccJerk.mean.X], 
AVG([fourierBodyAccJerk.mean.Y]) AS [fourierBodyAccJerk.mean.Y], 
AVG([fourierBodyAccJerk.mean.Z]) AS [fourierBodyAccJerk.mean.Z], 
AVG([fourierBodyAccJerk.stdev.X]) AS [fourierBodyAccJerk.stdev.X],
AVG([fourierBodyAccJerk.stdev.Y]) AS [fourierBodyAccJerk.stdev.Y],
AVG([fourierBodyAccJerk.stdev.Z]) AS [fourierBodyAccJerk.stdev.Z],
AVG([fourierBodyAccJerk.meanFreq.X]) AS [fourierBodyAccJerk.meanFreq.X], 
AVG([fourierBodyAccJerk.meanFreq.Y]) AS [fourierBodyAccJerk.meanFreq.Y], 
AVG([fourierBodyAccJerk.meanFreq.Z]) AS [fourierBodyAccJerk.meanFreq.Z], 
AVG([fourierBodyGyro.mean.X]) AS [fourierBodyGyro.mean.X],
AVG([fourierBodyGyro.mean.Y]) AS [fourierBodyGyro.mean.Y],
AVG([fourierBodyGyro.mean.Z]) AS [fourierBodyGyro.mean.Z],
AVG([fourierBodyGyro.stdev.X]) AS [fourierBodyGyro.stdev.X], 
AVG([fourierBodyGyro.stdev.Y]) AS [fourierBodyGyro.stdev.Y], 
AVG([fourierBodyGyro.stdev.Z]) AS [fourierBodyGyro.stdev.Z], 
AVG([fourierBodyGyro.meanFreq.X]) AS [fourierBodyGyro.meanFreq.X],
AVG([fourierBodyGyro.meanFreq.Y]) AS [fourierBodyGyro.meanFreq.Y],
AVG([fourierBodyGyro.meanFreq.Z]) AS [fourierBodyGyro.meanFreq.Z],
AVG([fourierBodyAccMag.mean]) AS [fourierBodyAccMag.mean],
AVG([fourierBodyAccMag.stdev]) AS [fourierBodyAccMag.stdev], 
AVG([fourierBodyAccMag.meanFreq]) AS [fourierBodyAccMag.meanFreq],
AVG([fourierBodyBodyAccJerkMag.mean]) AS [fourierBodyBodyAccJerkMag.mean],
AVG([fourierBodyBodyAccJerkMag.stdev]) AS [fourierBodyBodyAccJerkMag.stdev], 
AVG([fourierBodyBodyAccJerkMag.meanFreq]) AS [fourierBodyBodyAccJerkMag.meanFreq],
AVG([fourierBodyBodyGyroMag.mean]) AS [fourierBodyBodyGyroMag.mean], 
AVG([fourierBodyBodyGyroMag.stdev]) AS [fourierBodyBodyGyroMag.stdev],
AVG([fourierBodyBodyGyroMag.meanFreq]) AS [fourierBodyBodyGyroMag.meanFreq], 
AVG([fourierBodyBodyGyroJerkMag.mean]) AS [fourierBodyBodyGyroJerkMag.mean], 
AVG([fourierBodyBodyGyroJerkMag.stdev]) AS [fourierBodyBodyGyroJerkMag.stdev],
AVG([fourierBodyBodyGyroJerkMag.meanFreq]) AS [fourierBodyBodyGyroJerkMag.meanFreq]
FROM dt_subjectyX 
GROUP BY SubjectID, ActivityName
"

# sqldf(sql)
# View(sqldf(sql))


# Write to output file.
fil_out <- paste(dir_src_data, "C03_Smartphone_Summary.txt", sep = "/")
write.table(sqldf(sql), file = fil_out, row.name=FALSE)





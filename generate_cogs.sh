#!/bin/bash
# Script to convert input tif image into cloud optimized format and export jpeg version
#
# Inputs Expected:
# $1 = input file to process
# $2 = number of bands in original file

infile=$1
numbands=$2
filename="${1%.*}"
jpegfile=$filename.jpg

# convert original tif to COG format
gdal_translate $infile tempfile.tif -of COG -CO compress=lzw
mv tempfile.tif $infile

if [ "$numbands" = "1" ] || [ "$numbands" = "3" ]; then
	# Generate jpeg from whatever bands are present... rgb color (3) or greyscale (1)
	gdal_translate $infile $jpegfile -of jpeg -CO quality=80
else
	# create 3-band jpeg from original 4-band input
	gdal_translate $infile $jpegfile -of jpeg -CO quality=80 -b 1 -b 2 -b 3
fi

rm *.xml

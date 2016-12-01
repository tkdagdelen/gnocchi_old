/**
 * A script for extracting a sample of SNPs from a directory of 'big VCFs' by
 * passing in a 'small VCF'. Find all the desired in SNPs in big VCFs by only
 * grabbing SNP if it appears in small VCF.
 *
 * This is useful because plink can only handle a (relatively) small number of
 * SNPs compared to Gnocchi.
 *
 */

val outputVCF = "/data/UK10K/filteredVCF5.vcf"
val refVCF = "/data/sgoel/noheader.vcf"

val allVCFDirectory = "/data/UK10K/*"
// Pass in any one of the files from data directory
val headerLines = sc.textFile("/data/UK10K/EGAD00001000740/EGAZ00001016583_3.ALSPAC.beagle.anno.csq.shapeit.20131101.vcf").filter(line => line.contains('#'))

// Removes header lines from VCFs and reference file
val vcfRDD = sc.textFile(allVCFDirectory).filter(line => !line.contains('#'))
val noHeader = vcfRDD.filter(line => !line.contains('#'))

val referenceFile = sc.textFile(refVCF).filter(line => !line.contains('#'))
val referenceSNPs = sc.broadcast(referenceFile.map(line => line.split("\t")(2)).collect.toSet)

// Check if row contains a SNP from our referenceSNPs set
val filteredSNPs = noHeader.filter(e => containsSNP(e.split("\t")(2), referenceSNPs.value)).distinct()
// Add back header, and write out
val outputFile = headerLines.union(filteredSNPs)
outputFile.repartition(1).saveAsTextFile(outputVCF)

def containsSNP(SNP: String, referenceLine: Set[String]) : Boolean = {
    referenceLine.contains(SNP)
}
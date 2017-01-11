#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <bzlib.h>
#include <omp.h>

int partition_range(const int global_start, const int global_end,
    const int numThreads, const int threadID, int *local_start, int *local_end)
{
    int global_length = global_end - global_start;

    int chunk_size = global_length / numThreads;
        
    int remainder = global_length - chunk_size * numThreads;

    if (threadID < remainder)
    {
        *local_start = global_start + threadID  * chunk_size + threadID;
        *local_end = *local_start + chunk_size + 1;
    }
    else
    {
        *local_start = global_start + threadID * chunk_size + remainder;
        *local_end = *local_start + chunk_size;
    }

    return 0;
}


int main(int argc, char **argv)
{
    // Start program timer
    double start_time = omp_get_wtime();

    // Open source and destination files
    FILE *source_p = fopen(argv[1], "r");
    FILE *dest_p = fopen("output.txt.bz2", "w");

    // Print reminder to include second argument
    if (argv[2] == NULL)
        printf("Please, include a second argument "
            "for the number of threads.\n");

    // Get number of threads from command line and set
    int numThreads = atoi(argv[2]);
    omp_set_num_threads(numThreads);
    
    // Initialize compression method parameters
    int blockSize = 9;
    int verbosity = 0;
    int workFactor = 0;
    unsigned int sourceLen = numThreads * blockSize * 100000;
    unsigned int destLen = 1.01 * sourceLen + 600;
    
    // Allocate memory for source and destination buffers and destLenArr
    char *source = (char *) malloc(sizeof(char) * sourceLen);
    char **dest = (char **) malloc(sizeof(char *) * numThreads);
    
    for (int i = 0; i < numThreads; ++i)
    	dest[i] = (char *) malloc(sizeof(char) * destLen);

    int *destLenArr = (int *) malloc(sizeof(int) * numThreads);

    while (1)
    {
    	// Read from file to buffer
	int bytesRead = fread(source, sizeof(char), sourceLen, source_p);

        // Break if there is nothing left to write
 	if (bytesRead == 0)
 	    break;

        // Cover cases where the number of bytes read is less than the
        // amount we were aiming to read
 	if (bytesRead < sourceLen)
 	    sourceLen = bytesRead;

        // Update destLen and destLenArr
        destLen = 1.01 * sourceLen + 600;
        for (int i = 0; i < numThreads; ++i)
            destLenArr[i] = destLen;

        // Perform parallel compression
        #pragma omp parallel for
        for (int i = 0; i < numThreads; ++i)
        {
            int local_start, local_end;
            
            partition_range(0, sourceLen, numThreads, i, &local_start, &local_end);
			
            int retValue = BZ2_bzBuffToBuffCompress(dest[i], &destLenArr[i],
                &source[local_start], local_end - local_start, blockSize,
                    verbosity, workFactor);

	    /* DEBUG            
            if (retValue == BZ_OK)
                printf("BZ_OK returned.\n");
            if (retValue == BZ_CONFIG_ERROR)
                printf("BZ_CONFIG_ERROR returned.\n");
            if (retValue == BZ_PARAM_ERROR)
                printf("BZ_PARAM_ERROR returned.\n");
            if (retValue == BZ_MEM_ERROR)
                printf("BZ_MEM_ERROR returned.\n");
            if (retValue == BZ_OUTBUFF_FULL)
                printf("BZ_OUTBUFF_FULL returned.\n");
            */
        }

        // Write compressed data to file
        for (int i = 0; i < numThreads; ++i)
            fwrite(dest[i], sizeof(char), destLenArr[i], dest_p);
    }

    free(source);
    free(dest);

    fclose(source_p);
    fclose(dest_p);

    // End program timer, compute runtime, and print
    double end_time = omp_get_wtime();

    double time = end_time - start_time;

    printf("Program run-time (s): %lf\n", time);
}

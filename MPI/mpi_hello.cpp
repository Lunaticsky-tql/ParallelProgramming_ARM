
#include <cstdio>
#include "/usr/local/include/mpi.h"
//#include <mpi.h>

int main (int argc, char **argv)
{
    int rank, size;

    MPI_Init (&argc, &argv);  /* starts MPI */
    for (int i = 0; i < 4; ++i) {


        MPI_Comm_rank (MPI_COMM_WORLD, &rank);    /* get current process id */
        MPI_Comm_size (MPI_COMM_WORLD, &size);    /* get number of processes */

        printf( "Hello world from process %d of %d\n", rank, size );


    }

    MPI_Finalize();
    return 0;
}
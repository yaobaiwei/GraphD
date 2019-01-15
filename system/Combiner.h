#ifndef COMBINER_H
#define COMBINER_H

#include <iostream>
#include <mpi.h>
using namespace std;

template <class MessageT>
class Combiner {
public:
    virtual void combine(MessageT& old, const MessageT& new_msg)
    {
    	//default behavior is to terminate the program
    	//as there's no way of cancelling the combining effect
    	cout<<"ERROR: base class of Combiner calls combine(); note that combine() should be called only by subclass!"<<endl;
    	MPI_Abort(MPI_COMM_WORLD, 1);
    }
};

#endif

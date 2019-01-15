#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <stddef.h>

template <class VertexT, class PartialT, class FinalT>
class Aggregator {
public:
    typedef VertexT VertexType;
    typedef PartialT PartialType;
    typedef FinalT FinalType;
    typedef typename VertexT::EdgeType EdgeT;
    typedef vector<EdgeT> EdgeContainer;

    virtual void init() = 0;
    virtual void stepPartial(VertexT* v, EdgeContainer & edges) = 0;
    virtual void stepFinal(PartialT* part) = 0;
    virtual PartialT* finishPartial() = 0;
    virtual FinalT* finishFinal() = 0;
};

template <class VertexT>
class DummyAgg : public Aggregator<VertexT, char, char> {
public:
    typedef typename VertexT::EdgeType EdgeT;
    typedef vector<EdgeT> EdgeContainer;
    
    virtual void init()
    {
    }
    virtual void stepPartial(VertexT* v, EdgeContainer & edge)
    {
    }
    virtual void stepFinal(char* par)
    {
    }
    virtual char* finishPartial()
    {
        return NULL;
    }
    virtual char* finishFinal()
    {
        return NULL;
    }
};

#endif


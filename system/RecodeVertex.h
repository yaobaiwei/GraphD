#ifndef RECODEVERTEX_Hd
#define RECODEVERTEX_H

//changes compared with Vertex.h
//1. KeyT is fixed to VertexID, and so does HashT
//2. OldKeyT is recorded for easy dump, though it's totally fine to drop it (so as to consume less memory)
//3. currently, an adjacency list does not keep old neighbor ID, though it can be added if necessary

#include "global.h"
#include "Msg.h"
#include "serialization.h"
#include "ioser.h"
#include "Combiner.h"
#include "Vertex.h" //for DefaultHash
using namespace std;

template <class OldKeyT, class ValueT, class EdgeT, class MessageT, class CombinerT>
class RecodeVertex {
public:
	VertexID id; //keep it so that vid can be accessed in compute(.)
	OldKeyT old_id;
    ValueT value;
    bool active;
    int degree;

    typedef VertexID KeyType; //required by IDMsg
    typedef ValueT ValueType;
    typedef EdgeT EdgeType;
    typedef MessageT MessageType;
    typedef DefaultHash<VertexID> HashType; //fixed hash type
    typedef CombinerT CombinerType;
    typedef vector<MessageT> MessageContainer;
    typedef vector<EdgeT> EdgeContainer;
    typedef RecodeVertex<OldKeyT, ValueT, EdgeT, MessageT, CombinerT> VertexT;

    RecodeVertex() : active(true){}

    /* //no longer needed, as local vertices are ordered already
    inline bool operator<(const VertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const VertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const VertexT& rhs) const
    {
        return id != rhs.id;
    }
    */

    inline void activate()
    {
        active = true;
    }
    inline void vote_to_halt()
    {
        active = false;
    }

    virtual void compute(MessageT agg_msg, EdgeContainer& edges) = 0; //we do not make agg_msg a reference-type, to avoid users from changing it

    void send_message(const VertexID & id, const MessageT& msg)
    {
    	hasMsg(); //cannot end yet even every vertex halts
    	//prepare content (in fact, it's IDMsg)
    	mem_bytes << id;
    	mem_bytes << msg;
    	//add to msg-buf
    	DefaultHash<VertexID> hash;
    	append(hash(id)); //in_mem_stream is consumed
    }

    //---

    //non-recoded V-stream --> recoded in-mem v-object
	friend ifbinstream& operator>>(ifbinstream& m, VertexT& v) //note that v.id should be set to v-pos outside
	{
		m >> v.old_id >> v.value >> v.active >> v.degree;
		return m;
	}

    //other serialization functions are not necessary
};

#endif

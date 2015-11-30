/*-------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/index.h,v 1.73 2007/01/09 02:14:15 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

#include "access/relscan.h"     /* Relation, Snapshot */
#include "executor/tuptable.h"  /* TupTableSlot */
#include "nodes/execnodes.h"

struct EState;                  /* #include "nodes/execnodes.h" */

#define DEFAULT_INDEX_TYPE	"btree"

/* Typedef for callback function for IndexBuildScan */
typedef void (*IndexBuildCallback) (Relation index,
									ItemPointer tupleId,
									Datum *values,
									bool *isnull,
									bool tupleIsAlive,
									void *state);


extern Oid index_create(Oid heapRelationId,
			 const char *indexRelationName,
			 Oid indexRelationId,
			 IndexInfo *indexInfo,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *classObjectId,
			 int16 *coloptions,
			 Datum reloptions,
			 bool isprimary,
			 bool isconstraint,
			 Oid *constrOid,
			 bool allow_system_table_mods,
			 bool skip_build,
			 bool concurrent,
			 const char *altConName);

extern void index_drop(Oid indexId);

extern IndexInfo *BuildIndexInfo(Relation index);

extern void FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   struct EState *estate,
			   Datum *values,
			   bool *isnull);

extern void index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary);

extern double IndexBuildScan(Relation heapRelation,
				   Relation indexRelation,
				   IndexInfo *indexInfo,
				   IndexBuildCallback callback,
				   void *callback_state);

extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot);

extern Oid reindex_index(Oid indexId, Oid newrelfilenode, List **extra_oids);
extern bool reindex_relation(Oid relid, bool toast_too, bool aoseg_too, 
							 bool aoblkdir_too, bool aovisimap_too,
							 List **oidmap, bool build_map);

extern Oid IndexGetRelation(Oid indexId);

#endif   /* INDEX_H */

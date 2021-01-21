//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicIndexGet2DynamicIndexScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformDynamicIndexGet2DynamicIndexScan.h"

#include "gpopt/operators/CLogicalDynamicIndexGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalDynamicIndexScan.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan(
	CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDynamicIndexGet(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // index lookup predicate
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDynamicIndexGet2DynamicIndexScan::Transform(
	CXformContext *pxfctxt GPOS_ASSERTS_ONLY, CXformResult *pxfres GPOS_UNUSED,
	CExpression *pexpr GPOS_ASSERTS_ONLY) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDynamicIndexGet *popIndexGet =
		CLogicalDynamicIndexGet::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(mp) CName(mp, popIndexGet->Name());
	GPOS_ASSERT(pname != NULL);

	// extract components
	CExpression *pexprIndexCond = (*pexpr)[0];
	if (pexprIndexCond->DeriveHasSubquery())
	{
		return;
	}
	pexprIndexCond->AddRef();

	CTableDescriptor *ptabdesc = popIndexGet->Ptabdesc();
	ptabdesc->AddRef();

	CIndexDescriptor *pindexdesc = popIndexGet->Pindexdesc();
	pindexdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popIndexGet->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	CColRef2dArray *pdrgpdrgpcrPart = popIndexGet->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();

	COrderSpec *pos = popIndexGet->Pos();
	pos->AddRef();

	popIndexGet->GetPartitionMdids()->AddRef();
	popIndexGet->GetRootColMappingPerPart()->AddRef();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CPhysicalDynamicIndexScan(
						mp, pindexdesc, ptabdesc, pexpr->Pop()->UlOpId(), pname,
						pdrgpcrOutput, popIndexGet->ScanId(), pdrgpdrgpcrPart,
						pos, popIndexGet->GetPartitionMdids(),
						popIndexGet->GetRootColMappingPerPart()),
					pexprIndexCond);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

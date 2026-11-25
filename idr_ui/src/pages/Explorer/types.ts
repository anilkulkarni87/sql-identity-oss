import { api } from '../../api/client'

export interface ClusterSummary {
    resolved_id: string
    cluster_size: number
    confidence_score: number | null
}

export type InspectorTab = 'golden' | 'edges' | 'entity'

export type ClusterDetailData = Awaited<ReturnType<typeof api.getCluster>>
export type CrosswalkEntry = NonNullable<ClusterDetailData['crosswalk_history']>[number]
export type LensSummary = Awaited<ReturnType<typeof api.listLenses>>['lenses'][number]
export type PrivacyForgetResponse = Awaited<ReturnType<typeof api.privacyForget>>

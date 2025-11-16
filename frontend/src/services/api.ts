import axios, { AxiosRequestConfig } from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Helper function to create axios config with AbortController
export const createRequestConfig = (signal?: AbortSignal): AxiosRequestConfig => {
  return signal ? { signal } : {};
};

export interface ContactInformation {
  street_address?: string;
  city?: string;
  state?: string;
  zip?: string;
  email?: string;
  phone?: string;
  website?: string;
}

export interface Candidate {
  candidate_id: string;
  name: string;
  office?: string;
  party?: string;
  state?: string;
  district?: string;
  election_years?: number[];
  active_through?: number;
  contact_info?: ContactInformation;
  contact_info_updated_at?: string; // ISO format timestamp
}

export interface FinancialSummary {
  candidate_id: string;
  cycle?: number;
  total_receipts: number;
  total_disbursements: number;
  cash_on_hand: number;
  total_contributions: number;
  individual_contributions: number;
  pac_contributions: number;
  party_contributions: number;
  loan_contributions?: number;
}

export interface Contribution {
  contribution_id?: string;
  candidate_id?: string;
  committee_id?: string;
  contributor_name?: string;
  contributor_city?: string;
  contributor_state?: string;
  contributor_zip?: string;
  contributor_employer?: string;
  contributor_occupation?: string;
  contribution_amount: number;
  contribution_date?: string;
  contribution_type?: string;
  receipt_type?: string;
}

export interface ContributionAnalysis {
  total_contributions: number;
  total_contributors: number;
  average_contribution: number;
  contributions_by_date: Record<string, number>;
  contributions_by_state: Record<string, number>;
  top_donors: Array<{ name: string; total: number; count: number }>;
  contribution_distribution: Record<string, number>;
}

export interface FraudPattern {
  pattern_type: string;
  severity: 'low' | 'medium' | 'high';
  description: string;
  affected_contributions: Contribution[];
  total_amount: number;
  confidence_score: number;
}

export interface FraudAnalysis {
  candidate_id: string;
  patterns: FraudPattern[];
  risk_score: number;
  total_suspicious_amount: number;
  aggregated_donors_count?: number;
  aggregation_enabled?: boolean;
}

export interface MoneyFlowNode {
  id: string;
  name: string;
  type: 'candidate' | 'committee' | 'donor';
  amount?: number;
}

export interface MoneyFlowEdge {
  source: string;
  target: string;
  amount: number;
  type?: string;
}

export interface MoneyFlowGraph {
  nodes: MoneyFlowNode[];
  edges: MoneyFlowEdge[];
}

export const candidateApi = {
  search: async (params: {
    name?: string;
    office?: string;
    state?: string;
    party?: string;
    year?: number;
    limit?: number;
  }, signal?: AbortSignal): Promise<Candidate[]> => {
    const response = await api.get('/api/candidates/search', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getById: async (candidateId: string, signal?: AbortSignal): Promise<Candidate> => {
    const response = await api.get(`/api/candidates/${candidateId}`, createRequestConfig(signal));
    return response.data;
  },

  refreshContactInfo: async (candidateId: string, signal?: AbortSignal): Promise<{
    success: boolean;
    message: string;
    contact_info?: ContactInformation;
    contact_info_updated_at?: string;
  }> => {
    const response = await api.post(`/api/candidates/${candidateId}/refresh-contact-info`, undefined, createRequestConfig(signal));
    return response.data;
  },

  getFinancials: async (candidateId: string, cycle?: number, signal?: AbortSignal): Promise<FinancialSummary[]> => {
    const response = await api.get(`/api/candidates/${candidateId}/financials`, {
      params: cycle ? { cycle } : {},
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  getBatchFinancials: async (candidateIds: string[], cycle?: number, signal?: AbortSignal): Promise<Record<string, FinancialSummary[]>> => {
    const response = await api.post('/api/candidates/financials/batch', {
      candidate_ids: candidateIds,
      ...(cycle ? { cycle } : {}),
    }, createRequestConfig(signal));
    return response.data;
  },

  getRaceCandidates: async (params: {
    office: string;
    state: string;
    district?: string;
    year?: number;
    limit?: number;
  }, signal?: AbortSignal): Promise<Candidate[]> => {
    const response = await api.get('/api/candidates/race', { params, ...createRequestConfig(signal) });
    return response.data;
  },
};

export interface UniqueContributor {
  name: string;
  total_amount: number;
  contribution_count: number;
}

export interface AggregatedDonor {
  donor_key: string;
  canonical_name: string;
  canonical_state?: string;
  canonical_city?: string;
  canonical_employer?: string;
  canonical_occupation?: string;
  total_amount: number;
  contribution_count: number;
  first_contribution_date?: string;
  last_contribution_date?: string;
  contribution_ids: string[];
  all_names: string[];
  match_confidence: number;
}

export const contributionApi = {
  get: async (params: {
    candidate_id?: string;
    committee_id?: string;
    contributor_name?: string;
    min_amount?: number;
    max_amount?: number;
    min_date?: string;
    max_date?: string;
    limit?: number;
  }, signal?: AbortSignal): Promise<Contribution[]> => {
    const response = await api.get('/api/contributions/', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getUniqueContributors: async (searchTerm: string, limit?: number, signal?: AbortSignal): Promise<UniqueContributor[]> => {
    const response = await api.get('/api/contributions/unique-contributors', {
      params: { search_term: searchTerm, limit },
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  getAggregatedDonors: async (params: {
    candidate_id?: string;
    committee_id?: string;
    contributor_name?: string;
    min_amount?: number;
    max_amount?: number;
    min_date?: string;
    max_date?: string;
    limit?: number;
  }, signal?: AbortSignal): Promise<AggregatedDonor[]> => {
    const response = await api.get('/api/contributions/aggregated-donors', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  analyze: async (params: {
    candidate_id?: string;
    committee_id?: string;
    min_date?: string;
    max_date?: string;
    cycle?: number;
  }, signal?: AbortSignal): Promise<ContributionAnalysis> => {
    const response = await api.get('/api/contributions/analysis', { params, ...createRequestConfig(signal) });
    return response.data;
  },
};

export interface ExpenditureBreakdown {
  total_expenditures: number;
  total_transactions: number;
  average_expenditure: number;
  expenditures_by_date: Record<string, number>;
  expenditures_by_category: Record<string, number>;
  expenditures_by_recipient: Array<{ name: string; amount: number }>;
  top_recipients: Array<{ name: string; total: number; count: number }>;
}

export interface EmployerAnalysis {
  total_by_employer: Record<string, number>;
  top_employers: Array<{ employer: string; total: number; count: number }>;
  employer_count: number;
  total_contributions: number;
}

export interface ContributionVelocity {
  velocity_by_date: Record<string, number>;
  velocity_by_week: Record<string, number>;
  peak_days: Array<{ date: string; amount: number; count: number }>;
  average_daily_velocity: number;
}

export const analysisApi = {
  getMoneyFlow: async (candidateId: string, maxDepth?: number, minAmount?: number, aggregateByEmployer?: boolean, signal?: AbortSignal): Promise<MoneyFlowGraph> => {
    const response = await api.get('/api/analysis/money-flow', {
      params: {
        candidate_id: candidateId,
        max_depth: maxDepth,
        min_amount: minAmount,
        aggregate_by_employer: aggregateByEmployer,
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  getExpenditureBreakdown: async (params: {
    candidate_id?: string;
    committee_id?: string;
    min_date?: string;
    max_date?: string;
  }, signal?: AbortSignal): Promise<ExpenditureBreakdown> => {
    const response = await api.get('/api/analysis/expenditure-breakdown', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getEmployerBreakdown: async (params: {
    candidate_id?: string;
    committee_id?: string;
    min_date?: string;
    max_date?: string;
  }, signal?: AbortSignal): Promise<EmployerAnalysis> => {
    const response = await api.get('/api/analysis/employer-breakdown', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getVelocity: async (params: {
    candidate_id?: string;
    committee_id?: string;
    min_date?: string;
    max_date?: string;
  }, signal?: AbortSignal): Promise<ContributionVelocity> => {
    const response = await api.get('/api/analysis/velocity', { params, ...createRequestConfig(signal) });
    return response.data;
  },
};

export const fraudApi = {
  analyze: async (candidateId: string, minDate?: string, maxDate?: string, signal?: AbortSignal): Promise<FraudAnalysis> => {
    const response = await api.get('/api/fraud/analyze', {
      params: {
        candidate_id: candidateId,
        min_date: minDate,
        max_date: maxDate,
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  analyzeWithAggregation: async (
    candidateId: string,
    minDate?: string,
    maxDate?: string,
    useAggregation: boolean = true,
    signal?: AbortSignal
  ): Promise<FraudAnalysis> => {
    const response = await api.get('/api/fraud/analyze-donors', {
      params: {
        candidate_id: candidateId,
        min_date: minDate,
        max_date: maxDate,
        use_aggregation: useAggregation,
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },
};

export interface ApiKeyStatus {
  has_key: boolean;
  key_preview?: string;  // Masked key (e.g., "abcd...xyz1")
  source: 'ui' | 'env';
}

export interface IndependentExpenditure {
  expenditure_id?: string;
  cycle?: number;
  committee_id?: string;
  candidate_id?: string;
  candidate_name?: string;
  support_oppose_indicator?: string;
  expenditure_amount: number;
  expenditure_date?: string;
  payee_name?: string;
  expenditure_purpose?: string;
}

export interface IndependentExpenditureAnalysis {
  total_expenditures: number;
  total_support: number;
  total_oppose: number;
  total_transactions: number;
  expenditures_by_date: Record<string, number>;
  expenditures_by_committee: Record<string, number>;
  expenditures_by_candidate: Record<string, number>;
  top_committees: Array<{ committee_id: string; total_amount: number; count: number }>;
  top_candidates: Array<{ candidate_id: string; total_amount: number; count: number }>;
}

export interface CommitteeSummary {
  committee_id: string;
  name: string;
  committee_type?: string;
  committee_type_full?: string;
  party?: string;
  state?: string;
  candidate_ids?: string[];
}

export interface CommitteeFinancials {
  committee_id: string;
  cycle?: number;
  total_receipts: number;
  total_disbursements: number;
  cash_on_hand: number;
  total_contributions: number;
}

export interface CommitteeTransfer {
  transfer_id?: string;
  from_committee_id: string;
  to_committee_id?: string;
  amount: number;
  date?: string;
  purpose?: string;
}

export const committeeApi = {
  search: async (params: {
    name?: string;
    committee_type?: string;
    state?: string;
    limit?: number;
  }, signal?: AbortSignal): Promise<CommitteeSummary[]> => {
    const response = await api.get('/api/committees/search', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getById: async (committeeId: string, signal?: AbortSignal): Promise<CommitteeSummary> => {
    const response = await api.get(`/api/committees/${committeeId}`, createRequestConfig(signal));
    return response.data;
  },

  getFinancials: async (committeeId: string, cycle?: number, signal?: AbortSignal): Promise<CommitteeFinancials[]> => {
    const response = await api.get(`/api/committees/${committeeId}/financials`, {
      params: cycle ? { cycle } : {},
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  getContributions: async (
    committeeId: string,
    params?: {
      min_date?: string;
      max_date?: string;
      limit?: number;
    },
    signal?: AbortSignal
  ): Promise<Contribution[]> => {
    const response = await api.get(`/api/committees/${committeeId}/contributions`, { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getExpenditures: async (
    committeeId: string,
    params?: {
      min_date?: string;
      max_date?: string;
      limit?: number;
    },
    signal?: AbortSignal
  ): Promise<any[]> => {
    const response = await api.get(`/api/committees/${committeeId}/expenditures`, { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getTransfers: async (
    committeeId: string,
    params?: {
      min_date?: string;
      max_date?: string;
      limit?: number;
    },
    signal?: AbortSignal
  ): Promise<CommitteeTransfer[]> => {
    const response = await api.get(`/api/committees/${committeeId}/transfers`, { params, ...createRequestConfig(signal) });
    return response.data;
  },
};

export const independentExpenditureApi = {
  get: async (params: {
    candidate_id?: string;
    committee_id?: string;
    support_oppose?: string;
    min_date?: string;
    max_date?: string;
    min_amount?: number;
    max_amount?: number;
    limit?: number;
  }, signal?: AbortSignal): Promise<IndependentExpenditure[]> => {
    const response = await api.get('/api/independent-expenditures/', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  analyze: async (params: {
    candidate_id?: string;
    committee_id?: string;
    min_date?: string;
    max_date?: string;
  }, signal?: AbortSignal): Promise<IndependentExpenditureAnalysis> => {
    const response = await api.get('/api/independent-expenditures/analysis', { params, ...createRequestConfig(signal) });
    return response.data;
  },

  getCandidateSummary: async (candidateId: string, minDate?: string, maxDate?: string, signal?: AbortSignal): Promise<any> => {
    const response = await api.get(`/api/independent-expenditures/${candidateId}/summary`, {
      params: {
        min_date: minDate,
        max_date: maxDate,
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },
};

export interface BulkDataStatus {
  enabled: boolean;
  available_cycles: number[];
  total_records: number;
  update_interval_hours: number;
  background_updates_running: boolean;
  error?: string;
}

export interface BulkDataCycle {
  cycle: number;
  download_date?: string;
  record_count: number;
  file_path?: string;
  imported?: boolean;
}

export interface BulkImportJobStatus {
  job_id: string;
  job_type: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  cycle?: number;
  cycles?: number[];
  total_cycles: number;
  completed_cycles: number;
  current_cycle?: number;
  total_records: number;
  imported_records: number;
  skipped_records: number;
  current_chunk: number;
  total_chunks: number;
  error_message?: string;
  started_at?: string;
  completed_at?: string;
  progress_data?: any;
  overall_progress: number;
}

export interface DataTypeStatus {
  data_type: string;
  description: string;
  file_format: string;
  priority: number;
  is_implemented: boolean;
  status: 'imported' | 'not_imported' | 'failed' | 'in_progress';
  record_count: number;
  last_imported_at?: string | null;
  download_date?: string | null;
  error_message?: string | null;
}

export interface CycleStatus {
  cycle: number;
  data_types: DataTypeStatus[];
  count: number;
}

export const bulkDataApi = {
  getStatus: async (): Promise<BulkDataStatus> => {
    const response = await api.get('/api/bulk-data/status');
    return response.data;
  },

  getCycles: async (): Promise<{ cycles: BulkDataCycle[]; count: number }> => {
    const response = await api.get('/api/bulk-data/cycles');
    return response.data;
  },

  refreshCycles: async (): Promise<{ message: string; cycles: number[]; count: number }> => {
    const response = await api.post('/api/bulk-data/cycles/refresh');
    return response.data;
  },

  download: async (cycle: number, forceDownload: boolean = false): Promise<{ message: string; cycle: number; job_id: string; status: string }> => {
    const response = await api.post('/api/bulk-data/download', null, {
      params: { cycle, force_download: forceDownload },
    });
    return response.data;
  },

  clearContributions: async (cycle?: number): Promise<{ message: string; deleted_count: number; cycle?: number }> => {
    const response = await api.delete('/api/bulk-data/contributions', {
      params: cycle ? { cycle } : {},
    });
    return response.data;
  },

  cleanupAndReimport: async (cycle: number): Promise<{ message: string; cycle: number; job_id: string; status: string }> => {
    const response = await api.post('/api/bulk-data/cleanup-and-reimport', null, {
      params: { cycle },
    });
    return response.data;
  },

  importAll: async (startYear?: number, endYear?: number): Promise<{ message: string; cycles: number[]; count: number; job_id: string; status: string }> => {
    const response = await api.post('/api/bulk-data/import-all', null, {
      params: {
        ...(startYear ? { start_year: startYear } : {}),
        ...(endYear ? { end_year: endYear } : {}),
      },
    });
    return response.data;
  },
  getDataTypeStatus: async (cycle: number): Promise<CycleStatus> => {
    const response = await api.get(`/api/bulk-data/status/${cycle}`);
    return response.data;
  },
  importMultipleDataTypes: async (
    cycle: number,
    dataTypes: string[],
    forceDownload: boolean = false
  ): Promise<{ message: string; data_types: string[]; cycle: number; job_id: string; status: string }> => {
    // FastAPI expects array query params as repeated keys: ?data_types=val1&data_types=val2
    // axios by default uses brackets: ?data_types[]=val1&data_types[]=val2
    // So we need to build the query string manually or use paramsSerializer
    const params = new URLSearchParams();
    params.append('cycle', cycle.toString());
    params.append('force_download', forceDownload.toString());
    dataTypes.forEach(dt => params.append('data_types', dt));
    
    const response = await api.post(`/api/bulk-data/import-multiple?${params.toString()}`, null);
    return response.data;
  },
  importAllDataTypes: async (
    cycle: number
  ): Promise<{ message: string; cycle: number; job_id: string; status: string }> => {
    const response = await api.post('/api/bulk-data/import-all-types', null, {
      params: {
        cycle,
      },
    });
    return response.data;
  },

  getJobStatus: async (jobId: string): Promise<BulkImportJobStatus> => {
    const response = await api.get(`/api/bulk-data/jobs/${jobId}/status`);
    return response.data;
  },

  cancelJob: async (jobId: string): Promise<{ message: string; job_id: string; status: string }> => {
    const response = await api.post(`/api/bulk-data/jobs/${jobId}/cancel`);
    return response.data;
  },

  createWebSocket: (jobId: string, onMessage: (data: any) => void, onError?: (error: Event) => void): WebSocket | null => {
    try {
      // Determine WebSocket URL
      let wsUrl: string;
      const apiUrl = import.meta.env.VITE_API_URL;
      
      if (apiUrl) {
        // Use VITE_API_URL if set, converting http(s) to ws(s)
        wsUrl = apiUrl.replace(/^http/, 'ws').replace(/^https/, 'wss');
        wsUrl = `${wsUrl}/api/bulk-data/ws/${jobId}`;
      } else {
        // Fallback to same origin
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsHost = window.location.host;
        wsUrl = `${wsProtocol}//${wsHost}/api/bulk-data/ws/${jobId}`;
      }
      
      const ws = new WebSocket(wsUrl);
      
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          onMessage(data);
        } catch (e) {
          console.error('Error parsing WebSocket message:', e);
        }
      };
      
      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        if (onError) onError(error);
      };
      
      ws.onclose = () => {
        // WebSocket closed
      };
      
      return ws;
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
      if (onError) onError(error as Event);
      return null;
    }
  },
};

export interface TrendAnalysis {
  candidate_id: string;
  trends: Array<{
    cycle: number;
    total_receipts: number;
    total_disbursements: number;
    cash_on_hand: number;
    total_contributions: number;
    individual_contributions: number;
    pac_contributions: number;
    party_contributions?: number;
    loan_contributions?: number;
    receipts_growth?: number;
  }>;
  total_cycles: number;
}

export interface SavedSearch {
  id: number;
  name: string;
  search_type: string;
  search_params: Record<string, any>;
  created_at: string;
  updated_at: string;
}

export const trendApi = {
  getCandidateTrends: async (
    candidateId: string,
    minCycle?: number,
    maxCycle?: number,
    signal?: AbortSignal
  ): Promise<TrendAnalysis> => {
    const response = await api.get(`/api/trends/candidate/${candidateId}`, {
      params: {
        ...(minCycle ? { min_cycle: minCycle } : {}),
        ...(maxCycle ? { max_cycle: maxCycle } : {}),
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  getRaceTrends: async (
    candidateIds: string[],
    minCycle?: number,
    maxCycle?: number,
    signal?: AbortSignal
  ): Promise<any> => {
    const response = await api.post('/api/trends/race', {
      candidate_ids: candidateIds,
      ...(minCycle ? { min_cycle: minCycle } : {}),
      ...(maxCycle ? { max_cycle: maxCycle } : {}),
    }, createRequestConfig(signal));
    return response.data;
  },

  getContributionTrends: async (
    candidateId: string,
    minCycle?: number,
    maxCycle?: number,
    signal?: AbortSignal
  ): Promise<any> => {
    const response = await api.get(`/api/trends/contribution-velocity/${candidateId}`, {
      params: {
        ...(minCycle ? { min_cycle: minCycle } : {}),
        ...(maxCycle ? { max_cycle: maxCycle } : {}),
      },
      ...createRequestConfig(signal),
    });
    return response.data;
  },
};

export const savedSearchApi = {
  list: async (searchType?: string, signal?: AbortSignal): Promise<SavedSearch[]> => {
    const response = await api.get('/api/saved-searches/', {
      params: searchType ? { search_type: searchType } : {},
      ...createRequestConfig(signal),
    });
    return response.data;
  },

  create: async (
    name: string,
    searchType: string,
    searchParams: Record<string, any>
  ): Promise<SavedSearch> => {
    const response = await api.post('/api/saved-searches/', {
      name,
      search_type: searchType,
      search_params: searchParams,
    });
    return response.data;
  },

  get: async (searchId: number): Promise<SavedSearch> => {
    const response = await api.get(`/api/saved-searches/${searchId}`);
    return response.data;
  },

  update: async (
    searchId: number,
    name?: string,
    searchParams?: Record<string, any>
  ): Promise<SavedSearch> => {
    const response = await api.put(`/api/saved-searches/${searchId}`, {
      ...(name ? { name } : {}),
      ...(searchParams ? { search_params: searchParams } : {}),
    });
    return response.data;
  },

  delete: async (searchId: number): Promise<void> => {
    await api.delete(`/api/saved-searches/${searchId}`);
  },
};

export const settingsApi = {
  getApiKey: async (): Promise<ApiKeyStatus> => {
    const response = await api.get('/api/settings/api-key');
    return response.data;
  },
  
  setApiKey: async (apiKey: string): Promise<void> => {
    await api.post('/api/settings/api-key', { api_key: apiKey });
  },
  
  deleteApiKey: async (): Promise<void> => {
    await api.delete('/api/settings/api-key');
  },
};

export const exportApi = {
  exportCandidate: async (candidateId: string, format: 'pdf' | 'docx' | 'md' | 'csv' | 'excel', cycle?: number): Promise<void> => {
    const params: any = { format };
    if (cycle) {
      params.cycle = cycle;
    }
    
    const response = await api.get(`/api/export/candidate/${candidateId}`, {
      params,
      responseType: 'blob',
    });
    
    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    
    let extension = format;
    if (format === 'excel') extension = 'xlsx';
    else if (format === 'md') extension = 'md';
    
    link.setAttribute('download', `candidate_${candidateId}_report.${extension}`);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  },

  exportRace: async (
    candidateIds: string[],
    office: string,
    state: string,
    format: 'pdf' | 'docx' | 'md' | 'csv' | 'excel',
    district?: string,
    year?: number
  ): Promise<void> => {
    const response = await api.post(
      '/api/export/race',
      {
        candidate_ids: candidateIds,
        office,
        state,
        district,
        year,
        format,
      },
      {
        responseType: 'blob',
      }
    );
    
    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    
    let extension = format;
    if (format === 'excel') extension = 'xlsx';
    else if (format === 'md') extension = 'md';
    
    let filename = `race_${office}_${state}_report.${extension}`;
    if (district) {
      filename = `race_${office}_${state}_${district}_report.${extension}`;
    }
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  },

  exportContributions: async (
    format: 'csv' | 'excel',
    params?: {
      candidate_id?: string;
      committee_id?: string;
      contributor_name?: string;
      min_amount?: number;
      max_amount?: number;
      min_date?: string;
      max_date?: string;
      limit?: number;
    }
  ): Promise<void> => {
    const endpoint = format === 'excel' ? '/api/export/contributions/excel' : '/api/export/contributions/csv';
    
    const response = await api.get(endpoint, {
      params,
      responseType: 'blob',
    });
    
    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    
    const extension = format === 'excel' ? 'xlsx' : 'csv';
    link.setAttribute('download', `contributions_export.${extension}`);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  },
};

export default api;


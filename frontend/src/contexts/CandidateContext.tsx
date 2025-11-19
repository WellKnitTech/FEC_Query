import { createContext, useContext, ReactNode } from 'react';
import { Candidate, FinancialSummary } from '../services/api';

interface CandidateContextValue {
  candidateId: string | undefined;
  candidate: Candidate | null;
  cycle: number | undefined;
  setCycle: (cycle: number | undefined) => void;
  financials: FinancialSummary[];
  latestFinancial: FinancialSummary | null;
  selectedFinancial: FinancialSummary | null;
  availableCycles: number[];
}

export const CandidateContext = createContext<CandidateContextValue | undefined>(undefined);

interface CandidateContextProviderProps {
  children: ReactNode;
  candidateId: string | undefined;
  candidate: Candidate | null;
  cycle: number | undefined;
  setCycle: (cycle: number | undefined) => void;
  financials: FinancialSummary[];
  latestFinancial: FinancialSummary | null;
  selectedFinancial: FinancialSummary | null;
  availableCycles: number[];
}

export function CandidateContextProvider({
  children,
  candidateId,
  candidate,
  cycle,
  setCycle,
  financials,
  latestFinancial,
  selectedFinancial,
  availableCycles,
}: CandidateContextProviderProps) {
  const value: CandidateContextValue = {
    candidateId,
    candidate,
    cycle,
    setCycle,
    financials,
    latestFinancial,
    selectedFinancial,
    availableCycles,
  };

  return <CandidateContext.Provider value={value}>{children}</CandidateContext.Provider>;
}

export function useCandidateContext(): CandidateContextValue {
  const context = useContext(CandidateContext);
  if (context === undefined) {
    throw new Error('useCandidateContext must be used within a CandidateContextProvider');
  }
  return context;
}


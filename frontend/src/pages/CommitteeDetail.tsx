import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { committeeApi, CommitteeSummary, CommitteeFinancials, CommitteeTransfer, Contribution } from '../services/api';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

export default function CommitteeDetail() {
  const { committeeId } = useParams<{ committeeId: string }>();
  const navigate = useNavigate();
  const [committee, setCommittee] = useState<CommitteeSummary | null>(null);
  const [financials, setFinancials] = useState<CommitteeFinancials[]>([]);
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [expenditures, setExpenditures] = useState<any[]>([]);
  const [transfers, setTransfers] = useState<CommitteeTransfer[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'contributions' | 'expenditures' | 'transfers'>('overview');

  useEffect(() => {
    if (committeeId) {
      loadCommitteeData();
    }
  }, [committeeId]);

  const loadCommitteeData = async () => {
    if (!committeeId) return;
    
    setLoading(true);
    setError(null);

    try {
      const [committeeData, financialsData, contributionsData, expendituresData, transfersData] = await Promise.all([
        committeeApi.getById(committeeId),
        committeeApi.getFinancials(committeeId),
        committeeApi.getContributions(committeeId, { limit: 1000 }),
        committeeApi.getExpenditures(committeeId, { limit: 1000 }),
        committeeApi.getTransfers(committeeId, { limit: 500 }),
      ]);

      setCommittee(committeeData);
      setFinancials(financialsData);
      setContributions(contributionsData);
      setExpenditures(expendituresData);
      setTransfers(transfersData);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load committee data');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error || !committee) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded">
          {error || 'Committee not found'}
        </div>
      </div>
    );
  }

  const latestFinancials = financials.length > 0 ? financials.sort((a, b) => (b.cycle || 0) - (a.cycle || 0))[0] : null;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <button
        onClick={() => navigate(-1)}
        className="mb-4 text-blue-600 hover:text-blue-800"
      >
        ← Back
      </button>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h1 className="text-3xl font-bold text-gray-900 mb-4">{committee.name}</h1>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <span className="text-gray-600">Committee ID:</span>
            <p className="font-medium">{committee.committee_id}</p>
          </div>
          <div>
            <span className="text-gray-600">Type:</span>
            <p className="font-medium">{committee.committee_type_full || committee.committee_type || 'N/A'}</p>
          </div>
          <div>
            <span className="text-gray-600">Party:</span>
            <p className="font-medium">{committee.party || 'N/A'}</p>
          </div>
          <div>
            <span className="text-gray-600">State:</span>
            <p className="font-medium">{committee.state || 'N/A'}</p>
          </div>
        </div>
        {committee.candidate_ids && committee.candidate_ids.length > 0 && (
          <div className="mt-4">
            <span className="text-gray-600">Linked Candidates:</span>
            <div className="flex flex-wrap gap-2 mt-2">
              {committee.candidate_ids.map((candidateId) => (
                <button
                  key={candidateId}
                  onClick={() => navigate(`/candidate/${candidateId}`)}
                  className="px-3 py-1 bg-blue-100 text-blue-700 rounded hover:bg-blue-200 text-sm"
                >
                  {candidateId}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {committee.contact_info && (
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Contact Information</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {(committee.contact_info.street_address || committee.contact_info.city || committee.contact_info.zip) && (
              <div>
                <h3 className="text-sm font-medium text-gray-500 mb-1">Address</h3>
                <p className="text-gray-900">
                  {committee.contact_info.street_address && <>{committee.contact_info.street_address}<br /></>}
                  {committee.contact_info.street_address_2 && <>{committee.contact_info.street_address_2}<br /></>}
                  {committee.contact_info.city && committee.contact_info.state && committee.contact_info.zip && (
                    <>{committee.contact_info.city}, {committee.contact_info.state} {committee.contact_info.zip}</>
                  )}
                  {committee.contact_info.city && !committee.contact_info.state && (
                    <>{committee.contact_info.city}</>
                  )}
                </p>
              </div>
            )}
            {committee.contact_info.email && (
              <div>
                <h3 className="text-sm font-medium text-gray-500 mb-1">Email</h3>
                <a href={`mailto:${committee.contact_info.email}`} className="text-blue-600 hover:text-blue-800">
                  {committee.contact_info.email}
                </a>
              </div>
            )}
            {committee.contact_info.phone && (
              <div>
                <h3 className="text-sm font-medium text-gray-500 mb-1">Phone</h3>
                <a href={`tel:${committee.contact_info.phone}`} className="text-gray-900">
                  {committee.contact_info.phone}
                </a>
              </div>
            )}
            {committee.contact_info.website && (
              <div>
                <h3 className="text-sm font-medium text-gray-500 mb-1">Website</h3>
                <a 
                  href={committee.contact_info.website.startsWith('http') ? committee.contact_info.website : `https://${committee.contact_info.website}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800"
                >
                  {committee.contact_info.website}
                </a>
              </div>
            )}
            {committee.contact_info.treasurer_name && (
              <div>
                <h3 className="text-sm font-medium text-gray-500 mb-1">Treasurer</h3>
                <p className="text-gray-900">{committee.contact_info.treasurer_name}</p>
              </div>
            )}
          </div>
        </div>
      )}

      {latestFinancials && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Total Receipts</div>
            <div className="text-2xl font-bold">${(latestFinancials.total_receipts / 1000).toFixed(1)}K</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Total Disbursements</div>
            <div className="text-2xl font-bold">${(latestFinancials.total_disbursements / 1000).toFixed(1)}K</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Cash on Hand</div>
            <div className="text-2xl font-bold">${(latestFinancials.cash_on_hand / 1000).toFixed(1)}K</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Total Contributions</div>
            <div className="text-2xl font-bold">${(latestFinancials.total_contributions / 1000).toFixed(1)}K</div>
          </div>
        </div>
      )}

      <div className="bg-white rounded-lg shadow">
        <div className="border-b border-gray-200">
          <nav className="flex -mb-px">
            {(['overview', 'contributions', 'expenditures', 'transfers'] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-6 py-3 text-sm font-medium border-b-2 ${
                  activeTab === tab
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </nav>
        </div>

        <div className="p-6">
          {activeTab === 'overview' && (
            <div>
              <h2 className="text-xl font-semibold mb-4">Financial Summary</h2>
              {financials.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cycle</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Receipts</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Disbursements</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cash on Hand</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Contributions</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {financials.map((fin) => (
                        <tr key={fin.cycle}>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{fin.cycle || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">${(fin.total_receipts / 1000).toFixed(1)}K</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">${(fin.total_disbursements / 1000).toFixed(1)}K</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">${(fin.cash_on_hand / 1000).toFixed(1)}K</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">${(fin.total_contributions / 1000).toFixed(1)}K</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-gray-500">No financial data available</p>
              )}
            </div>
          )}

          {activeTab === 'contributions' && (
            <div>
              <h2 className="text-xl font-semibold mb-4">Contributions Received ({contributions.length})</h2>
              {contributions.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Contributor</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Location</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {contributions.slice(0, 100).map((contrib, idx) => (
                        <tr key={idx}>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{contrib.contribution_date || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{contrib.contributor_name || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">${contrib.contribution_amount.toLocaleString()}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">
                            {contrib.contributor_city && contrib.contributor_state
                              ? `${contrib.contributor_city}, ${contrib.contributor_state}`
                              : 'N/A'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-gray-500">No contributions data available</p>
              )}
            </div>
          )}

          {activeTab === 'expenditures' && (
            <div>
              <h2 className="text-xl font-semibold mb-4">Expenditures Made ({expenditures.length})</h2>
              {expenditures.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Recipient</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Purpose</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {expenditures.slice(0, 100).map((exp, idx) => (
                        <tr key={idx}>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{exp.expenditure_date || exp.disbursement_date || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{exp.recipient_name || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">${(exp.expenditure_amount || exp.disbursement_amount || 0).toLocaleString()}</td>
                          <td className="px-6 py-4 text-sm">{exp.expenditure_purpose || 'N/A'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-gray-500">No expenditures data available</p>
              )}
            </div>
          )}

          {activeTab === 'transfers' && (
            <div>
              <h2 className="text-xl font-semibold mb-4">Committee Transfers ({transfers.length})</h2>
              {transfers.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">To Committee</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Purpose</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {transfers.map((transfer, idx) => (
                        <tr key={idx}>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">{transfer.date || 'N/A'}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm">
                            {transfer.to_committee_id ? (
                              <button
                                onClick={() => navigate(`/committee/${transfer.to_committee_id}`)}
                                className="text-blue-600 hover:text-blue-800"
                              >
                                {transfer.to_committee_id}
                              </button>
                            ) : (
                              'N/A'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">${transfer.amount.toLocaleString()}</td>
                          <td className="px-6 py-4 text-sm">{transfer.purpose || 'N/A'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-gray-500">No transfers data available</p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}


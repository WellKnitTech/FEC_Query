import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import CandidateSearch from '../components/CandidateSearch'
import { candidateApi } from '../services/api'

// Mock the API
vi.mock('../services/api', () => ({
  candidateApi: {
    search: vi.fn(),
  },
}))

describe('CandidateSearch', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders search form', () => {
    render(
      <BrowserRouter>
        <CandidateSearch />
      </BrowserRouter>
    )
    
    expect(screen.getByPlaceholderText(/search for candidates/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /search/i })).toBeInTheDocument()
  })

  it('handles search with mocked API', async () => {
    const mockCandidates = [
      {
        candidate_id: 'P00003392',
        name: 'Test Candidate',
        office: 'P',
        party: 'DEM',
        state: 'DC',
      },
    ]

    vi.mocked(candidateApi.search).mockResolvedValue(mockCandidates)

    const user = userEvent.setup()
    render(
      <BrowserRouter>
        <CandidateSearch />
      </BrowserRouter>
    )

    const input = screen.getByPlaceholderText(/search for candidates/i)
    const button = screen.getByRole('button', { name: /search/i })

    await user.type(input, 'Test')
    await user.click(button)

    await waitFor(() => {
      expect(candidateApi.search).toHaveBeenCalledWith({ name: 'Test', limit: 20 })
    })

    await waitFor(() => {
      expect(screen.getByText('Test Candidate')).toBeInTheDocument()
    })
  })

  it('displays error message when search fails', async () => {
    vi.mocked(candidateApi.search).mockRejectedValue(new Error('API Error'))

    const user = userEvent.setup()
    render(
      <BrowserRouter>
        <CandidateSearch />
      </BrowserRouter>
    )

    const input = screen.getByPlaceholderText(/search for candidates/i)
    const button = screen.getByRole('button', { name: /search/i })

    await user.type(input, 'Test')
    await user.click(button)

    await waitFor(() => {
      expect(screen.getByText(/failed to search/i)).toBeInTheDocument()
    })
  })
})


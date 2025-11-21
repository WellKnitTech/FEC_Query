import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import App from '../App'

describe('App', () => {
  it('renders without crashing', () => {
    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )
    
    // Check that the main navigation is present
    expect(screen.getByText(/FEC Campaign Finance Analysis/i)).toBeInTheDocument()
  })

  it('renders navigation links', () => {
    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )
    
    // Check for navigation links
    expect(screen.getByText(/Search/i)).toBeInTheDocument()
    expect(screen.getByText(/Race Analysis/i)).toBeInTheDocument()
  })
})


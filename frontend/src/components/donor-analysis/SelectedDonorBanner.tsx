interface SelectedDonorBannerProps {
  selectedContributor: string;
  onBack: () => void;
}

export default function SelectedDonorBanner({
  selectedContributor,
  onBack,
}: SelectedDonorBannerProps) {
  return (
    <div className="mb-4 p-4 bg-blue-50 border border-blue-200 rounded">
      <div className="flex justify-between items-center">
        <span className="text-blue-800">
          Showing contributions for: <strong>{selectedContributor}</strong>
        </span>
        <button
          onClick={onBack}
          className="text-blue-600 hover:text-blue-800 text-sm underline"
        >
          Change contributor
        </button>
      </div>
    </div>
  );
}


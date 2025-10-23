export function Footer() {
  return (
    <footer className="border-t border-border-low bg-white">
      <div className="max-w-7xl mx-auto px-8 py-12">
        <p className="text-body-md text-gray-600">
          Built with{' '}
          <a
            href="https://github.com/anza-xyz/jetstreamer"
            target="_blank"
            rel="noreferrer"
            className="text-gray-900 hover:underline underline-offset-4 transition-colors"
          >
            Jetstreamer
          </a>
          . Powered by Solana blockchain data.
        </p>
      </div>
    </footer>
  );
}


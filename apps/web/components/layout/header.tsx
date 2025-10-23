import Link from 'next/link';
import { Activity } from 'lucide-react';

export function Header() {
  return (
    <header className="relative z-50 w-full border-b border-border-low bg-white/80 backdrop-blur">
      <div className="max-w-7xl mx-auto px-8">
        <div className="flex h-16 items-center justify-between">
          <Link href="/" className="flex items-center space-x-2">
            <Activity className="h-5 w-5 text-gray-900" />
            <span className="font-diatype-medium text-lg text-gray-900">Solana Analytics</span>
          </Link>

          <nav className="flex items-center space-x-8">
            <Link
              href="/programs"
              className="text-nav-item text-gray-600 hover:text-gray-900 transition-colors"
            >
              Programs
            </Link>
            <Link
              href="/network"
              className="text-nav-item text-gray-600 hover:text-gray-900 transition-colors"
            >
              Network
            </Link>
            <Link
              href="/transactions"
              className="text-nav-item text-gray-600 hover:text-gray-900 transition-colors"
            >
              Transactions
            </Link>
          </nav>
        </div>
      </div>
    </header>
  );
}


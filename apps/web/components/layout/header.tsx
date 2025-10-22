import Link from 'next/link';
import { Activity } from 'lucide-react';

export function Header() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-16 items-center">
        <Link href="/" className="flex items-center space-x-2">
          <Activity className="h-6 w-6" />
          <span className="font-bold text-xl">Solana Analytics</span>
        </Link>

        <nav className="ml-auto flex items-center space-x-6 text-sm font-medium">
          <Link
            href="/programs"
            className="transition-colors hover:text-foreground/80"
          >
            Programs
          </Link>
          <Link
            href="/network"
            className="transition-colors hover:text-foreground/80"
          >
            Network
          </Link>
          <Link
            href="/transactions"
            className="transition-colors hover:text-foreground/80"
          >
            Transactions
          </Link>
        </nav>
      </div>
    </header>
  );
}


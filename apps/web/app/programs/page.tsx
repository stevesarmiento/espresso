import { Suspense } from 'react';
import { ProgramList } from '@/components/programs/program-list';
import { ProgramListSkeleton } from '@/components/programs/program-list-skeleton';

export const metadata = {
  title: 'Top Solana Programs | Analytics',
  description: 'Discover the most active programs on Solana blockchain',
};

export default function ProgramsPage() {
  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold mb-2">Top Solana Programs</h1>
        <p className="text-muted-foreground">
          Real-time analytics from the Solana blockchain
        </p>
      </div>

      <Suspense fallback={<ProgramListSkeleton />}>
        <ProgramList timeRange="24h" />
      </Suspense>
    </div>
  );
}


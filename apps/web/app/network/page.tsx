import { Suspense } from 'react';
import { CurrentStats } from '@/components/network/current-stats';
import { LiveFeed } from '@/components/network/live-feed';
import { Skeleton } from '@/components/ui/skeleton';

export const metadata = {
  title: 'Network Health | Solana Analytics',
  description: 'Monitor Solana network health and performance metrics',
};

export default function NetworkPage() {
  return (
    <div className="min-h-screen bg-bg1 overflow-x-clip relative">
      {/* Gradient from top */}
      <div className="absolute top-0 left-0 right-0 h-[400px] bg-gradient-to-b from-white/50 to-transparent pointer-events-none z-0" />
      
      <div className="relative z-10">
        <div className="max-w-7xl mx-auto border-r border-l border-border-low">
          {/* Header Section */}
          <section className="relative py-16 px-8 border-b border-border-low">
            <svg 
              className="absolute inset-0 pointer-events-none z-10"
              style={{
                left: '50%',
                top: '0%',
                transform: 'translate(-50%, -50%)',
                width: '150vw',
                height: '100vh',
              }}
            >
              <defs>
                <linearGradient id="networkHeaderPulse" x1="0%" y1="0%" x2="100%" y2="0%">
                  <stop offset="0%" stopColor="transparent" />
                  <stop offset="50%" stopColor="rgba(0, 0, 0, 0.8)" />
                  <stop offset="100%" stopColor="transparent" />
                </linearGradient>
              </defs>
              
              <line 
                x1="0" 
                y1="50%" 
                x2="100%" 
                y2="50%" 
                stroke="rgba(233, 231, 222, 1)" 
                strokeWidth="1"
              />
              
              <rect 
                x="0" 
                y="50%" 
                width="2%" 
                height="1" 
                fill="url(#networkHeaderPulse)"
                transform="translate(0, -0.5)"
              >
                <animate
                  attributeName="x"
                  values="0; 100%; 100%"
                  dur="6s"
                  begin="0.5s"
                  repeatCount="indefinite"
                />
                <animate
                  attributeName="opacity"
                  values="0; 0.8; 0.8; 0"
                  dur="6s"
                  begin="0.5s"
                  repeatCount="indefinite"
                />
              </rect>
            </svg>

            <div className="relative z-20">
              <h1 className="text-h2 mb-3">Network Health</h1>
              <p className="text-body-l text-gray-600">
                Real-time network performance metrics
              </p>
            </div>
          </section>

          {/* Stats Section */}
          <section className="relative py-16 px-8">
            <div className="absolute inset-0 left-[50%] -translate-x-1/2 w-screen z-[0] bg-gradient-to-t from-sand-200/50 to-transparent" />
            
            <div className="max-w-7xl mx-auto relative z-10">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Current Stats - Big prominent numbers */}
                <Suspense fallback={<Skeleton className="h-96 w-full" />}>
                  <CurrentStats />
                </Suspense>

                {/* Live Feed - Streaming slots */}
                <Suspense fallback={<Skeleton className="h-96 w-full" />}>
                  <LiveFeed />
                </Suspense>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}


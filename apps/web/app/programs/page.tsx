import { Suspense } from 'react';
import { ProgramList } from '@/components/programs/program-list';
import { ProgramListSkeleton } from '@/components/programs/program-list-skeleton';

export const metadata = {
  title: 'Top Solana Programs | Analytics',
  description: 'Discover the most active programs on Solana blockchain',
};

export default function ProgramsPage() {
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
                <linearGradient id="programsHeaderPulse" x1="0%" y1="0%" x2="100%" y2="0%">
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
                fill="url(#programsHeaderPulse)"
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
              <h1 className="text-h2 mb-3">Top Solana Programs</h1>
              <p className="text-body-l text-gray-600">
                Real-time analytics from the Solana blockchain
              </p>
            </div>
          </section>

          {/* Programs Grid Section */}
          <section className="relative py-16">
            <div className="absolute inset-0 left-[50%] -translate-x-1/2 w-screen z-[0] bg-gradient-to-t from-sand-200/50 to-transparent" />
            
            <svg 
              className="absolute inset-0 pointer-events-none z-10"
              style={{
                left: '50%',
                top: '100%',
                transform: 'translate(-50%, -50%)',
                width: '150vw',
                height: '100vh',
              }}
            >
              <defs>
                <linearGradient id="programsBottomPulse" x1="0%" y1="0%" x2="100%" y2="0%">
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
                fill="url(#programsBottomPulse)"
                transform="translate(0, -0.5)"
              >
                <animate
                  attributeName="x"
                  values="0; 100%; 100%"
                  dur="6s"
                  begin="3.5s"
                  repeatCount="indefinite"
                />
                <animate
                  attributeName="opacity"
                  values="0; 0.8; 0.8; 0"
                  dur="6s"
                  begin="3.5s"
                  repeatCount="indefinite"
                />
              </rect>
            </svg>

            <div className="max-w-7xl mx-auto relative z-10 px-8">
              <Suspense fallback={<ProgramListSkeleton />}>
                <ProgramList timeRange="24h" />
              </Suspense>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}


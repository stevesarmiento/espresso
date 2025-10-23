import Link from 'next/link';
import { Activity, Database, Zap, TrendingUp } from 'lucide-react';

export default function HomePage() {
  return (
    <div className="min-h-screen bg-bg1 overflow-x-clip relative">
      {/* Gradient from top to bottom */}
      <div className="absolute top-0 left-0 right-0 h-[600px] bg-gradient-to-b from-white/50 to-transparent pointer-events-none z-0" />
      
      <div className="relative z-10">
        <div className="max-w-7xl mx-auto border-r border-l border-border-low">
          {/* Hero Section */}
          <section className="relative py-24 px-8">
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
                <linearGradient id="heroHorizontalPulse" x1="0%" y1="0%" x2="100%" y2="0%">
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
                fill="url(#heroHorizontalPulse)"
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

            <div className="flex flex-col items-center text-center relative z-20">
              <h1 className="text-h1 mb-6">
                Solana Analytics Platform
              </h1>
              <p className="text-body-xl text-gray-600 max-w-2xl mb-12">
                Real-time blockchain analytics powered by Jetstreamer. Track programs,
                monitor network health, and explore transactions.
              </p>
              <div className="flex gap-4">
                <Link 
                  href="/programs"
                  className="px-8 py-3 bg-black text-white rounded-lg hover:bg-gray-900 transition-colors font-medium"
                >
                  Explore Programs
                </Link>
                <Link 
                  href="/network"
                  className="px-8 py-3 border border-border-medium rounded-lg hover:bg-sand-100 transition-colors font-medium"
                >
                  Network Stats
                </Link>
              </div>
            </div>
          </section>

          {/* Features Section */}
          <section className="relative py-16">
            <div className="absolute inset-0 left-[50%] -translate-x-1/2 w-screen z-[0] bg-gradient-to-t from-sand-200/50 to-transparent" />
            
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
                <linearGradient id="featuresHorizontalPulse" x1="0%" y1="0%" x2="100%" y2="0%">
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
                fill="url(#featuresHorizontalPulse)"
                transform="translate(0, -0.5)"
              >
                <animate
                  attributeName="x"
                  values="0; 100%; 100%"
                  dur="6s"
                  begin="1.5s"
                  repeatCount="indefinite"
                />
                <animate
                  attributeName="opacity"
                  values="0; 0.8; 0.8; 0"
                  dur="6s"
                  begin="1.5s"
                  repeatCount="indefinite"
                />
              </rect>
            </svg>

            <div className="max-w-7xl mx-auto relative z-10 px-8">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mt-[-0.5px]">
                <FeatureCard
                  icon={<Activity className="h-8 w-8" />}
                  title="Program Analytics"
                  description="Track the most active Solana programs with detailed transaction metrics and success rates."
                />
                <FeatureCard
                  icon={<Zap className="h-8 w-8" />}
                  title="Real-time TPS"
                  description="Monitor network performance with live transactions per second and throughput metrics."
                />
                <FeatureCard
                  icon={<Database className="h-8 w-8" />}
                  title="Transaction Search"
                  description="Search and analyze individual transactions with detailed compute unit usage."
                />
                <FeatureCard
                  icon={<TrendingUp className="h-8 w-8" />}
                  title="Historical Data"
                  description="Access historical program activity and network trends over time."
                />
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

function FeatureCard({ 
  icon, 
  title, 
  description 
}: { 
  icon: React.ReactNode; 
  title: string; 
  description: string;
}) {
  return (
    <div className="group relative p-6 border border-border-low bg-white hover:shadow-lg transition-all duration-200">
      <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
      <div className="relative z-10">
        <div className="mb-4 text-gray-900">{icon}</div>
        <h3 className="text-title-5 mb-2">{title}</h3>
        <p className="text-body-md text-gray-600">{description}</p>
      </div>
    </div>
  );
}


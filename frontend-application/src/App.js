import { useState } from 'react';
import './App.css';
import Feed from './components/Feed';
import Sidebar from './components/Sidebar';
import TrendingHashtags from './components/TrendingHashtags';
import Widgets from './components/Widgets';
function App() {
  const [activeState, setActiveState] = useState('HOME')
  return (
    <div className="app">
      <Sidebar setActive={setActiveState}/>
      {/* <ResponsiveDrawer/> */}
      <Feed activeState={activeState} setActiveState={setActiveState}/>
      <Widgets />
    </div>
  );
}

export default App;
